package org.openjdbcproxy.grpc.server;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.ReadLobRequest;
import com.openjdbcproxy.grpc.ResultType;
import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.SessionTerminationStatus;
import com.openjdbcproxy.grpc.StatementRequest;
import com.openjdbcproxy.grpc.StatementServiceGrpc;
import com.openjdbcproxy.grpc.TransactionInfo;
import com.openjdbcproxy.grpc.TransactionStatus;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.openjdbcproxy.constants.CommonConstants;
import org.openjdbcproxy.grpc.dto.OpQueryResult;
import org.openjdbcproxy.grpc.dto.Parameter;
import org.openjdbcproxy.grpc.dto.ParameterType;

import java.io.InputStream;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.openjdbcproxy.constants.CommonConstants.MAX_LOB_DATA_BLOCK_SIZE;
import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;
import static org.openjdbcproxy.grpc.server.Constants.EMPTY_STRING;
import static org.openjdbcproxy.grpc.server.Constants.H2_DRIVER_CLASS;
import static org.openjdbcproxy.grpc.server.Constants.OJP_DRIVER_PREFIX;
import static org.openjdbcproxy.grpc.server.Constants.POSTGRES_DRIVER_CLASS;
import static org.openjdbcproxy.grpc.server.Constants.SHA_256;
import static org.openjdbcproxy.grpc.server.GrpcExceptionHandler.sendSQLExceptionMetadata;

@RequiredArgsConstructor
public class StatementServiceImpl extends StatementServiceGrpc.StatementServiceImplBase {

    //TODO put the datasource at database level not user + database so if more than one user agaist the DB still maintain the max pool size
    private final Map<String, HikariDataSource> datasourceMap = new ConcurrentHashMap<>();
    private final SessionManager sessionManager;

    static {
        //TODO register all JDBC drivers supported here.
        try {
            Class.forName(H2_DRIVER_CLASS);
            Class.forName(POSTGRES_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void connect(ConnectionDetails connectionDetails, StreamObserver<SessionInfo> responseObserver) {
        String connHash = hashConnectionDetails(connectionDetails);
        System.out.println("connect connHash = " + connHash);

        HikariDataSource ds = this.datasourceMap.get(connHash);
        if (ds == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(this.parseUrl(connectionDetails.getUrl()));
            config.setUsername(connectionDetails.getUser());
            config.setPassword(connectionDetails.getPassword());
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            config.addDataSourceProperty("maximumPoolSize", 1);
            config.addDataSourceProperty("minimumPoolSize", 1);
            ds = new HikariDataSource(config);

            this.datasourceMap.put(connHash, ds);
        }

        this.sessionManager.registerClientUUID(connHash, connectionDetails.getClientUUID());

        responseObserver.onNext(SessionInfo.newBuilder()
                .setConnHash(connHash)
                .setClientUUID(connectionDetails.getClientUUID())
                .build()
        );
        responseObserver.onCompleted();
    }

    @Override
    public void executeUpdate(StatementRequest request, StreamObserver<OpResult> responseObserver) {
        Integer updated = 0;
        SessionInfo returnSessionInfo = request.getSession();

        try {
            ConnectionSessionDTO dto = sessionConnection(request.getSession(), false);
            Statement stmt = null;
            try {
                List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
                if (CollectionUtils.isNotEmpty(params) || StringUtils.isNotEmpty(request.getPreparedStatementUUID())) {
                    PreparedStatement ps;
                    if (StringUtils.isNotEmpty(request.getPreparedStatementUUID())) {
                        ps = sessionManager.getPreparedStatement(dto.getSession(), request.getPreparedStatementUUID());
                    } else {
                        ps = this.createPreparedStatement(dto, request.getSql(), params);
                    }
                    updated = ps.executeUpdate();
                    stmt = ps;
                } else {
                    stmt = dto.getConnection().createStatement();
                    updated = stmt.executeUpdate(request.getSql());
                }
            } catch (SQLException e) {
                sendSQLExceptionMetadata(e, responseObserver);
            } finally {
                //If there is no session, close statement and connection
                if (StringUtils.isEmpty(dto.getSession().getSessionUUID())) {
                    assert stmt != null;
                    stmt.close();
                    dto.getConnection().close();
                }
            }
        } catch (SQLException e) {// Need a second catch just for the acquisition of the connection
            sendSQLExceptionMetadata(e, responseObserver);
        }

        responseObserver.onNext(OpResult.newBuilder()
                .setType(ResultType.INTEGER)
                .setSession(returnSessionInfo)
                .setValue(ByteString.copyFrom(serialize(updated))).build());
        responseObserver.onCompleted();
    }

    private PreparedStatement createPreparedStatement(ConnectionSessionDTO dto, String sql, List<Parameter> params)
            throws SQLException {
        PreparedStatement ps = dto.getConnection().prepareStatement(sql);
        for (int i = 0; i < params.size(); i++) {
            Parameter parameter = params.get(i);
            this.addParam(dto.getSession(), parameter.getIndex(), ps, params.get(i));
        }
        return ps;
    }

    @Override
    public void executeQuery(StatementRequest request, StreamObserver<OpResult> responseObserver) {

        try {
            ConnectionSessionDTO dto = this.sessionConnection(request.getSession(), true);

            List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
            if (CollectionUtils.isNotEmpty(params)) {
                PreparedStatement ps = this.createPreparedStatement(dto, request.getSql(), params);
                String resultSetUUID = this.sessionManager.registerResultSet(dto.getSession(), ps.executeQuery());
                this.handleResultSet(dto.getSession(), resultSetUUID, responseObserver);
            } else {
                Statement stmt = dto.getConnection().createStatement();
                String resultSetUUID = this.sessionManager.registerResultSet(dto.getSession(),
                        stmt.executeQuery(request.getSql()));
                this.handleResultSet(dto.getSession(), resultSetUUID, responseObserver);
            }

        } catch (SQLException e) {
            sendSQLExceptionMetadata(e, responseObserver);
        }
    }

    @Override
    public StreamObserver<LobDataBlock> createLob(StreamObserver<LobReference> responseObserver) {

        return new ServerCallStreamObserver<>() {
            private SessionInfo sessionInfo;
            private String lobUUID;
            private LobType lobType;
            private LobDataBlocksInputStream lobDataBlocksInputStream = null;
            private final AtomicBoolean isFirstBlock = new AtomicBoolean(true);
            private final AtomicInteger countBytesWritten = new AtomicInteger(0);

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public void setOnCancelHandler(Runnable runnable) {

            }

            @Override
            public void setCompression(String s) {

            }

            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setOnReadyHandler(Runnable runnable) {

            }

            @Override
            public void request(int i) {

            }

            @Override
            public void setMessageCompression(boolean b) {

            }

            @Override
            public void disableAutoInboundFlowControl() {

            }

            @Override
            public void onNext(LobDataBlock lobDataBlock) {
                try {
                    this.lobType = lobDataBlock.getLobType();
                    ConnectionSessionDTO dto = sessionConnection(lobDataBlock.getSession(), true);
                    Connection conn = dto.getConnection();
                    if (StringUtils.isEmpty(lobDataBlock.getSession().getSessionUUID()) || this.lobUUID == null) {
                        if (LobType.LT_BLOB.equals(this.lobType)) {
                            Blob newBlob = conn.createBlob();
                            this.lobUUID = sessionManager.registerLob(dto.getSession(), newBlob);
                        } else if (LobType.LT_CLOB.equals(this.lobType)) {
                            Clob newClob = conn.createClob();
                            this.lobUUID = sessionManager.registerLob(dto.getSession(), newClob);
                        }
                    }

                    int bytesWritten = 0;
                    switch (this.lobType) {
                        case LT_BLOB -> {
                            Blob blob = sessionManager.getLob(dto.getSession(), this.lobUUID);
                            byte[] byteArrayData = lobDataBlock.getData().toByteArray();
                            bytesWritten = blob.setBytes(lobDataBlock.getPosition(), byteArrayData);
                        }
                        case LT_CLOB -> {
                            Clob clob = sessionManager.getLob(dto.getSession(), this.lobUUID);
                            byte[] byteArrayData = lobDataBlock.getData().toByteArray();
                            Writer writer = clob.setCharacterStream(lobDataBlock.getPosition());
                            writer.write(new String(byteArrayData, StandardCharsets.UTF_8).toCharArray());
                            bytesWritten = byteArrayData.length;
                        }
                        case LT_BINARY_STREAM -> {
                            if (this.lobUUID == null) {
                                byte[] metadataBytes = lobDataBlock.getMetadata().toByteArray();
                                if (metadataBytes == null && metadataBytes.length < 1) {
                                    throw new SQLException("Metadata empty for binary stream type.");
                                }
                                Map<Integer, Object> metadata = deserialize(metadataBytes, Map.class);
                                String sql = (String) metadata.get(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_SQL);
                                PreparedStatement ps;
                                String preparedStatementUUID = (String) metadata.get(CommonConstants.PREPARED_STATEMENT_UUID_BINARY_STREAM);
                                if (StringUtils.isNotEmpty(preparedStatementUUID)) {
                                    lobUUID = preparedStatementUUID;//TODO probably would be more readable to have a separate UUID, review this later, reads will have actual lob ids referring to inputstreams saved in session
                                    ps = sessionManager.getPreparedStatement(dto.getSession(), preparedStatementUUID);
                                } else {
                                    ps = dto.getConnection().prepareStatement(sql);
                                    lobUUID = sessionManager.registerPreparedStatement(dto.getSession(), ps);
                                }
                                //Need to first send the reff to the client before adding the stream as a parameter
                                sendLobRef(dto, lobDataBlock.getData().toByteArray().length);

                                //Add bite stream as parameter to the prepared statement
                                lobDataBlocksInputStream = new LobDataBlocksInputStream(lobDataBlock);
                                Integer parameterIndex = (Integer) metadata.get(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_INDEX);
                                Long length = (Long) metadata.get(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_LENGTH);
                                List<Object> values = new ArrayList<>();
                                values.add(lobDataBlocksInputStream);
                                if (length > -1) {
                                    values.add(length);
                                }
                                Parameter parameter = Parameter.builder()
                                        .index(parameterIndex)
                                        .type(ParameterType.BINARY_STREAM)
                                        .values(values)
                                        .build();
                                CompletableFuture.runAsync(() -> {
                                    try {
                                        addParam(dto.getSession(), parameter.getIndex(), ps, parameter);
                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                            } else {
                                lobDataBlocksInputStream.addBlock(lobDataBlock);
                            }
                        }
                    }
                    this.countBytesWritten.addAndGet(bytesWritten);
                    this.sessionInfo = dto.getSession();

                    if (isFirstBlock.get()) {
                        sendLobRef(dto, bytesWritten);
                    }

                } catch (SQLException e) {
                    sendSQLExceptionMetadata(e, responseObserver);
                } catch (Exception e) {
                    sendSQLExceptionMetadata(new SQLException("Unable to write data: " + e.getMessage(), e), responseObserver);
                }
            }

            private void sendLobRef(ConnectionSessionDTO dto, int bytesWritten) {
                //Send one flag response to indicate that the Blob has been created successfully and the first
                // block fo data has been written successfully.
                responseObserver.onNext(LobReference.newBuilder()
                        .setSession(dto.getSession())
                        .setUuid(this.lobUUID)
                        .setLobType(this.lobType)
                        .setBytesWritten(bytesWritten)
                        .build()
                );
                isFirstBlock.set(false);
            }

            @Override
            public void onError(Throwable throwable) {
                if (lobDataBlocksInputStream != null) {
                    lobDataBlocksInputStream.finish(true);
                }
            }

            @Override
            public void onCompleted() {
                if (lobDataBlocksInputStream != null) {
                    lobDataBlocksInputStream.finish(true);
                }
                //Send the final Lob reference with total count of written bytes.
                responseObserver.onNext(LobReference.newBuilder()
                        .setSession(this.sessionInfo)
                        .setUuid(this.lobUUID)
                        .setLobType(this.lobType)
                        .setBytesWritten(this.countBytesWritten.get())
                        .build()
                );
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void readLob(ReadLobRequest request, StreamObserver<LobDataBlock> responseObserver) {
        try {
            LobReference lobRef = request.getLobReference();
            ReadLobContext readLobContext = this.findLobContext(request);
            InputStream inputStream = readLobContext.getInputStream();
            //If the lob length is known the exact size of the next block is also known.
            boolean exactSizeKnown = readLobContext.getLobLength().isPresent() && readLobContext.getAvailableLength().isPresent();
            int nextByte = inputStream.read();
            int nextBlockSize = this.nextBlockSize(readLobContext, request.getPosition());
            byte[] nextBlock = new byte[nextBlockSize];
            int idx = -1;
            int currentPos = (int) request.getPosition();
            while (nextByte != -1) {
                nextBlock[++idx] = (byte) nextByte;
                if (idx == nextBlockSize - 1) {
                    currentPos += idx;
                    //Send data to client in limited size blocks to safeguard server memory.
                    responseObserver.onNext(LobDataBlock.newBuilder()
                            .setSession(lobRef.getSession())
                            .setPosition(currentPos + 1)
                            .setData(ByteString.copyFrom(nextBlock))
                            .build()
                    );
                    nextBlockSize = this.nextBlockSize(readLobContext, currentPos);
                    nextBlock = new byte[nextBlockSize];
                    idx = -1;
                }
                nextByte = inputStream.read();
            }

            //Send leftover bytes
            if (nextBlock.length > 0 && nextBlock[0] != -1) {
                byte[] adjustedSizeArray = (idx % MAX_LOB_DATA_BLOCK_SIZE != 0 && !exactSizeKnown) ?
                        trim(nextBlock) : nextBlock;
                currentPos = (int) request.getPosition() + idx;
                responseObserver.onNext(LobDataBlock.newBuilder()
                        .setSession(lobRef.getSession())
                        .setPosition(currentPos)
                        .setData(ByteString.copyFrom(adjustedSizeArray))
                        .build()
                );
            }

            responseObserver.onCompleted();

        } catch (SQLException se) {
            sendSQLExceptionMetadata(se, responseObserver);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] trim(byte[] nextBlock) {
        int lastBytePos = 0;
        for (int i = nextBlock.length - 1; i >= 0; i--) {
            int currentByte = nextBlock[i];
            if (currentByte != 0) {
                lastBytePos = i;
                break;
            }
        }

        byte[] trimmedArray = new byte[lastBytePos + 1];
        System.arraycopy(nextBlock, 0, trimmedArray, 0, lastBytePos + 1);
        return trimmedArray;
    }

    @Builder
    static class ReadLobContext {
        @Getter
        private InputStream inputStream;
        @Getter
        private Optional<Long> lobLength;
        @Getter
        private Optional<Integer> availableLength;
    }

    @SneakyThrows
    private ReadLobContext findLobContext(ReadLobRequest request) throws SQLException {
        InputStream inputStream = null;
        LobReference lobReference = request.getLobReference();
        ReadLobContext.ReadLobContextBuilder readLobContextBuilder = ReadLobContext.builder();
        switch (request.getLobReference().getLobType()) {
            case LobType.LT_BLOB -> {
                Blob blob = this.sessionManager.getLob(lobReference.getSession(), lobReference.getUuid());
                long lobLength = blob.length();
                readLobContextBuilder.lobLength(Optional.of(lobLength));
                int availableLength = (request.getPosition() + request.getLength()) < lobLength ? request.getLength() :
                        (int) (lobLength - request.getPosition() + 1);
                readLobContextBuilder.availableLength(Optional.of(availableLength));
                inputStream = blob.getBinaryStream(request.getPosition(), availableLength);
            }
            case LobType.LT_BINARY_STREAM -> {
                readLobContextBuilder.lobLength(Optional.empty());
                readLobContextBuilder.availableLength(Optional.empty());
                inputStream = sessionManager.getLob(lobReference.getSession(), lobReference.getUuid());
                inputStream.reset();//Might be a second read of the same stream, this guarantees that the position is at the start.
            }
        }
        readLobContextBuilder.inputStream(inputStream);

        return readLobContextBuilder.build();
    }

    private int nextBlockSize(ReadLobContext readLobContext, long position) {

        //BinaryStreams do not have means to know the size of the lob like Blobs or Clobs.
        if (readLobContext.getAvailableLength().isEmpty() || readLobContext.getLobLength().isEmpty()) {
            return MAX_LOB_DATA_BLOCK_SIZE;
        }

        long lobLength = readLobContext.getLobLength().get();
        int length = readLobContext.getAvailableLength().get();

        //Single read situations
        if ((int) lobLength == length && position == 1) {
            return length;
        }
        int nextBlockSize = Math.min(MAX_LOB_DATA_BLOCK_SIZE, length);
        int nextPos = (int) (position + nextBlockSize);
        if (nextPos > lobLength) {
            nextBlockSize = Math.toIntExact(nextBlockSize - (nextPos - lobLength));
        } else if ((position + 1) % length == 0) {
            nextBlockSize = 0;
        }

        return nextBlockSize;
    }

    @Override
    public void terminateSession(SessionInfo sessionInfo, StreamObserver<SessionTerminationStatus> responseObserver) {
        try {
            this.sessionManager.terminateSession(sessionInfo);
            responseObserver.onNext(SessionTerminationStatus.newBuilder().setTerminated(true).build());
            responseObserver.onCompleted();
        } catch (SQLException se) {
            sendSQLExceptionMetadata(se, responseObserver);
        } catch (Exception e) {
            sendSQLExceptionMetadata(new SQLException("Unable to terminate session: " + e.getMessage()), responseObserver);
        }
    }

    @Override
    public void startTransaction(SessionInfo sessionInfo, StreamObserver<SessionInfo> responseObserver) {
        System.out.println("Starting transaction");
        try {
            SessionInfo activeSessionInfo = sessionInfo;

            //Start a session if none started yet.
            if (StringUtils.isEmpty(sessionInfo.getSessionUUID())) {
                Connection conn = this.datasourceMap.get(sessionInfo.getConnHash()).getConnection();
                activeSessionInfo = sessionManager.createSession(sessionInfo.getClientUUID(), conn);
            }
            Connection sessionConnection = sessionManager.getConnection(activeSessionInfo);
            //Start a transaction
            sessionConnection.setAutoCommit(Boolean.FALSE);

            TransactionInfo transactionInfo = TransactionInfo.newBuilder()
                    .setTransactionStatus(TransactionStatus.TRX_ACTIVE)
                    .setTransactionUUID(UUID.randomUUID().toString())
                    .build();

            SessionInfo.Builder sessionInfoBuilder = this.newBuilderFrom(activeSessionInfo);
            sessionInfoBuilder.setTransactionInfo(transactionInfo);

            responseObserver.onNext(sessionInfoBuilder.build());
            responseObserver.onCompleted();
        } catch (SQLException se) {
            sendSQLExceptionMetadata(se, responseObserver);
        } catch (Exception e) {
            sendSQLExceptionMetadata(new SQLException("Unable to start transaction: " + e.getMessage()), responseObserver);
        }
    }

    @Override
    public void commitTransaction(SessionInfo sessionInfo, StreamObserver<SessionInfo> responseObserver) {
        System.out.println("Commiting transaction");
        try {
            Connection conn = sessionManager.getConnection(sessionInfo);
            conn.commit();

            TransactionInfo transactionInfo = TransactionInfo.newBuilder()
                    .setTransactionStatus(TransactionStatus.TRX_COMMITED)
                    .setTransactionUUID(sessionInfo.getTransactionInfo().getTransactionUUID())
                    .build();

            SessionInfo.Builder sessionInfoBuilder = this.newBuilderFrom(sessionInfo);
            sessionInfoBuilder.setTransactionInfo(transactionInfo);

            responseObserver.onNext(sessionInfoBuilder.build());
            responseObserver.onCompleted();
        } catch (SQLException se) {
            sendSQLExceptionMetadata(se, responseObserver);
        } catch (Exception e) {
            sendSQLExceptionMetadata(new SQLException("Unable to commit transaction: " + e.getMessage()), responseObserver);
        }
    }

    @Override
    public void rollbackTransaction(SessionInfo sessionInfo, StreamObserver<SessionInfo> responseObserver) {
        System.out.println("Rollback transaction");
        try {
            Connection conn = sessionManager.getConnection(sessionInfo);
            conn.rollback();

            TransactionInfo transactionInfo = TransactionInfo.newBuilder()
                    .setTransactionStatus(TransactionStatus.TRX_ROLLBACK)
                    .setTransactionUUID(sessionInfo.getTransactionInfo().getTransactionUUID())
                    .build();

            SessionInfo.Builder sessionInfoBuilder = this.newBuilderFrom(sessionInfo);
            sessionInfoBuilder.setTransactionInfo(transactionInfo);

            responseObserver.onNext(sessionInfoBuilder.build());
            responseObserver.onCompleted();
        } catch (SQLException se) {
            sendSQLExceptionMetadata(se, responseObserver);
        } catch (Exception e) {
            sendSQLExceptionMetadata(new SQLException("Unable to rollback transaction: " + e.getMessage()), responseObserver);
        }
    }

    private SessionInfo.Builder newBuilderFrom(SessionInfo activeSessionInfo) {
        return SessionInfo.newBuilder()
                .setConnHash(activeSessionInfo.getConnHash())
                .setClientUUID(activeSessionInfo.getClientUUID())
                .setSessionUUID(activeSessionInfo.getSessionUUID())
                .setSessionStatus(activeSessionInfo.getSessionStatus())
                .setTransactionInfo(activeSessionInfo.getTransactionInfo());
    }

    /**
     * Finds a suitable connection for the current sessionInfo.
     * If there is a connection already in the sessionInfo reuse it, if not ge a fresh one from the data source.
     *
     * @param sessionInfo        - current sessionInfo object.
     * @param startSessionIfNone - if true will start a new sessionInfo if none exists.
     * @return ConnectionSessionDTO
     * @throws SQLException if connection not found or closed (by timeout or other reason)
     */
    private ConnectionSessionDTO sessionConnection(SessionInfo sessionInfo, boolean startSessionIfNone) throws SQLException {
        ConnectionSessionDTO.ConnectionSessionDTOBuilder dtoBuilder = ConnectionSessionDTO.builder();
        dtoBuilder.session(sessionInfo);
        Connection conn;
        if (StringUtils.isNotEmpty(sessionInfo.getSessionUUID())) {
            conn = this.sessionManager.getConnection(sessionInfo);
            if (conn == null) {
                throw new SQLException("Connection not found for this sessionInfo");
            }
            if (conn.isClosed()) {
                throw new SQLException("Connection is closed");
            }
        } else {
            //TODO check why reaches here and can't find the datasource sometimes, conn hash should never change for a single client
            //System.out.println("Lookup connection hash -> " + sessionInfo.getConnHash());
            conn = this.datasourceMap.get(sessionInfo.getConnHash()).getConnection();
            if (startSessionIfNone) {
                SessionInfo updatedSession = this.sessionManager.createSession(sessionInfo.getClientUUID(), conn);
                dtoBuilder.session(updatedSession);
            }
        }
        dtoBuilder.connection(conn);

        return dtoBuilder.build();
    }

    private void handleResultSet(SessionInfo session, String resultSetUUID, StreamObserver<OpResult> responseObserver)
            throws SQLException {
        ResultSet rs = this.sessionManager.getResultSet(session, resultSetUUID);
        OpQueryResult.OpQueryResultBuilder queryResultBuilder = OpQueryResult.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        List<String> labels = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            labels.add(rs.getMetaData().getColumnName(i + 1));
        }
        queryResultBuilder.labels(labels);

        List<Object[]> results = new ArrayList<>();
        int row = 0;
        boolean justSent = false;
        while (rs.next()) {
            justSent = false;
            row++;
            Object[] rowValues = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                int colType = rs.getMetaData().getColumnType(i + 1);
                Object currentValue = null;
                //Postgres uses type BYTEA which translates to type VARBINARY
                switch (colType) {
                    case Types.BLOB -> {
                        Blob blob = rs.getBlob(i + 1);
                        currentValue = this.sessionManager.registerLob(session, blob);
                    }
                    case Types.CLOB -> {
                        Clob clob = rs.getClob(i + 1);
                        currentValue = this.sessionManager.registerLob(session, clob);
                    }
                    case Types.BINARY -> {
                        int precision = rs.getMetaData().getPrecision(i + 1);
                        String catalogName = rs.getMetaData().getCatalogName(i + 1);
                        if (precision == 1) { //it is a single byte
                            currentValue = rs.getByte(i + 1);
                        } else if (StringUtils.isNotEmpty(catalogName)) {
                            currentValue = rs.getBytes(i + 1);
                        } else {
                            InputStream inputStream = rs.getBinaryStream(i + 1);
                            currentValue = this.sessionManager.registerLob(session, inputStream);
                        }
                    }
                    default -> {
                        currentValue = rs.getObject(i + 1);
                    }
                }
                rowValues[i] = currentValue;
            }
            results.add(rowValues);

            if (row % CommonConstants.ROWS_PER_RESULT_SET_DATA_BLOCK == 0) {
                justSent = true;
                //Send a block of records
                responseObserver.onNext(this.wrapResults(session, results, queryResultBuilder, resultSetUUID));
                queryResultBuilder = OpQueryResult.builder();// Recreate the builder to not send labels in every block.
                results = new ArrayList<>();
            }
        }

        if (!justSent) {
            //Send a block of remaining records
            responseObserver.onNext(this.wrapResults(session, results, queryResultBuilder, resultSetUUID));
        }

        responseObserver.onCompleted();

    }

    private OpResult wrapResults(SessionInfo sessionInfo,
                                 List<Object[]> results,
                                 OpQueryResult.OpQueryResultBuilder queryResultBuilder,
                                 String resultSetUUID) {

        OpResult.Builder resultsBuilder = OpResult.newBuilder();
        resultsBuilder.setSession(sessionInfo);
        resultsBuilder.setType(ResultType.RESULT_SET_DATA);
        queryResultBuilder.resultSetUUID(resultSetUUID);
        queryResultBuilder.rows(results);
        resultsBuilder.setValue(ByteString.copyFrom(serialize(queryResultBuilder.build())));

        return resultsBuilder.build();
    }

    private void addParam(SessionInfo session, int idx, PreparedStatement ps, Parameter param) throws SQLException {
        switch (param.getType()) {
            case INT -> ps.setInt(idx, (int) param.getValues().getFirst());
            case DOUBLE -> ps.setDouble(idx, (double) param.getValues().getFirst());
            case STRING -> ps.setString(idx, (String) param.getValues().getFirst());
            case LONG -> ps.setLong(idx, (long) param.getValues().getFirst());
            case BOOLEAN -> ps.setBoolean(idx, (boolean) param.getValues().getFirst());
            case BIG_DECIMAL -> ps.setBigDecimal(idx, (BigDecimal) param.getValues().getFirst());
            case FLOAT -> ps.setFloat(idx, (float) param.getValues().getFirst());
            case BYTES -> ps.setBytes(idx, (byte[]) param.getValues().getFirst());
            case BYTE ->
                    ps.setByte(idx, ((byte[]) param.getValues().getFirst())[0]);//Comes as an array of bytes with one element.
            case DATE -> ps.setDate(idx, (Date) param.getValues().getFirst());
            case TIME -> ps.setTime(idx, (Time) param.getValues().getFirst());
            case TIMESTAMP -> ps.setTimestamp(idx, (Timestamp) param.getValues().getFirst());
            //LOB types
            case BLOB ->
                    ps.setBlob(idx, this.sessionManager.<Blob>getLob(session, (String) param.getValues().getFirst()));
            case CLOB -> {
                Clob clob = this.sessionManager.getLob(session, (String) param.getValues().getFirst());
                ps.setClob(idx, clob.getCharacterStream());
            }
            case BINARY_STREAM -> {
                InputStream is = (InputStream) param.getValues().getFirst();
                if (param.getValues().size() > 1) {
                    Long size = (Long) param.getValues().get(1);
                    ps.setBinaryStream(idx, is, size);
                } else {
                    ps.setBinaryStream(idx, is);
                }
            }
            default -> ps.setObject(idx, param.getValues().getFirst());
        }
    }

    private String parseUrl(String url) {
        if (url == null) {
            return url;
        }
        return url.replaceAll(OJP_DRIVER_PREFIX, EMPTY_STRING);
    }

    private String hashConnectionDetails(ConnectionDetails connectionDetails) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(SHA_256);
            messageDigest.update((connectionDetails.getUrl() + connectionDetails.getUser() + connectionDetails.getPassword())
                    .getBytes());
            return new String(messageDigest.digest());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}