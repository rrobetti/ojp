package org.openjdbcproxy.grpc.server;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
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
import com.openjdbcproxy.grpc.TargetCall;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.openjdbcproxy.constants.CommonConstants;
import org.openjdbcproxy.grpc.dto.OpQueryResult;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.io.InputStream;
import java.io.Writer;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
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
import static org.openjdbcproxy.grpc.server.Constants.EMPTY_LIST;
import static org.openjdbcproxy.grpc.server.Constants.EMPTY_MAP;
import static org.openjdbcproxy.grpc.server.Constants.EMPTY_STRING;
import static org.openjdbcproxy.grpc.server.Constants.H2_DRIVER_CLASS;
import static org.openjdbcproxy.grpc.server.Constants.POSTGRES_DRIVER_CLASS;
import static org.openjdbcproxy.grpc.server.Constants.SHA_256;
import static org.openjdbcproxy.grpc.server.GrpcExceptionHandler.sendSQLExceptionMetadata;

@Slf4j
@RequiredArgsConstructor
//TODO this became a GOD class, need to try to delegate some work to specialized other classes where possible, it is challenging because many GRPC callbacks rely on attributes present here to work.
public class StatementServiceImpl extends StatementServiceGrpc.StatementServiceImplBase {

    //TODO put the datasource at database level not user + database so if more than one user agaist the DB still maintain the max pool size
    private final Map<String, HikariDataSource> datasourceMap = new ConcurrentHashMap<>();
    private final SessionManager sessionManager;
    private final CircuitBreaker circuitBreaker;

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
        log.info("connect connHash = " + connHash);

        HikariDataSource ds = this.datasourceMap.get(connHash);
        if (ds == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(this.parseUrl(connectionDetails.getUrl()));
            config.setUsername(connectionDetails.getUser());
            config.setPassword(connectionDetails.getPassword());
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            config.addDataSourceProperty("maximumPoolSize", 5);
            config.addDataSourceProperty("minimumPoolSize", 2);
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

    @SneakyThrows
    @Override
    public void executeUpdate(StatementRequest request, StreamObserver<OpResult> responseObserver) {
        log.info("Executing update {}", request.getSql());
        String stmtHash = SqlStatementXXHash.hashSqlQuery(request.getSql());
        int updated = 0;
        SessionInfo returnSessionInfo = request.getSession();
        ConnectionSessionDTO dto = ConnectionSessionDTO.builder().build();

        Statement stmt = null;
        String psUUID = "";

        try {
            dto = sessionConnection(request.getSession(), this.isAddBatchOperation(request));

            List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
            PreparedStatement ps = dto.getSession() != null && StringUtils.isNotBlank(dto.getSession().getSessionUUID())
                    && StringUtils.isNoneBlank(request.getStatementUUID()) ?
                    sessionManager.getPreparedStatement(dto.getSession(), request.getStatementUUID()) : null;
            if (CollectionUtils.isNotEmpty(params) || ps != null) {
                if (StringUtils.isNotEmpty(request.getStatementUUID())) {
                    Collection<Object> lobs = sessionManager.getLobs(dto.getSession());
                    for (Object o : lobs) {
                        LobDataBlocksInputStream lobIS = (LobDataBlocksInputStream) o;
                        Map<String, Object> metadata = (Map<String, Object>) sessionManager.getAttr(dto.getSession(), lobIS.getUuid());
                        Integer parameterIndex = (Integer) metadata.get(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_INDEX);
                        ps.setBinaryStream(parameterIndex, lobIS);
                    }
                    sessionManager.waitLobStreamsConsumption(dto.getSession());
                    if (ps != null) {
                        this.addParametersPreparedStatement(dto, ps, params);
                    }
                } else {
                    ps = this.createPreparedStatement(dto, request.getSql(), params, request);
                }
                if (this.isAddBatchOperation(request)) {
                    ps.addBatch();
                    if (request.getStatementUUID().isBlank()) {
                        psUUID = sessionManager.registerPreparedStatement(dto.getSession(), ps);
                    } else {
                        psUUID = request.getStatementUUID();
                    }
                } else {
                    updated = ps.executeUpdate();
                }
                stmt = ps;
            } else {
                stmt = this.createStatement(dto.getConnection(), request);
                updated = stmt.executeUpdate(request.getSql());
            }

            if (this.isAddBatchOperation(request)) {
                responseObserver.onNext(OpResult.newBuilder()
                        .setType(ResultType.UUID_STRING)
                        .setSession(returnSessionInfo)
                        .setValue(ByteString.copyFrom(serialize(psUUID))).build());
            } else {
                responseObserver.onNext(OpResult.newBuilder()
                        .setType(ResultType.INTEGER)
                        .setSession(returnSessionInfo)
                        .setValue(ByteString.copyFrom(serialize(updated))).build());
            }
            responseObserver.onCompleted();
            circuitBreaker.onSuccess(stmtHash);
        } catch (SQLException e) {// Need a second catch just for the acquisition of the connection
            circuitBreaker.onFailure(stmtHash, e);
            log.error("Failure during update execution: " + e.getMessage(), e);
            sendSQLExceptionMetadata(e, responseObserver);
        } finally {
            //If there is no session, close statement and connection
            if (dto.getSession() == null || StringUtils.isEmpty(dto.getSession().getSessionUUID())) {
                assert stmt != null;
                try {
                    stmt.close();
                    stmt.getConnection().close();
                } catch (SQLException e) {
                    log.error("Failure closing statement or connection: " + e.getMessage(), e);
                }
            }
        }
    }

    private boolean isAddBatchOperation(StatementRequest request) {
        if (request.getProperties().isEmpty()) {
            return false;
        }
        Map<String, Object> properties = deserialize(request.getProperties().toByteArray(), Map.class);
        return (Boolean) properties.get(CommonConstants.PREPARED_STATEMENT_ADD_BATCH_FLAG);
    }

    private Statement createStatement(Connection connection, StatementRequest request) throws SQLException {
        if (StringUtils.isNotEmpty(request.getStatementUUID())) {
            return this.sessionManager.getStatement(request.getSession(), request.getStatementUUID());
        }
        if (request.getProperties().isEmpty()) {
            return connection.createStatement();
        }
        Map<String, Object> properties = deserialize(request.getParameters().toByteArray(), Map.class);

        if (properties.isEmpty()) {
            return connection.createStatement();
        }
        if (properties.size() == 2) {
            return connection.createStatement(
                    (Integer) properties.get(CommonConstants.STATEMENT_RESULT_SET_TYPE_KEY),
                    (Integer) properties.get(CommonConstants.STATEMENT_RESULT_SET_CONCURRENCY_KEY));
        }
        if (properties.size() == 3) {
            return connection.createStatement(
                    (Integer) properties.get(CommonConstants.STATEMENT_RESULT_SET_TYPE_KEY),
                    (Integer) properties.get(CommonConstants.STATEMENT_RESULT_SET_CONCURRENCY_KEY),
                    (Integer) properties.get(CommonConstants.STATEMENT_RESULT_SET_HOLDABILITY_KEY));
        }
        throw new SQLException("Incorrect number of properties for creating a new statement.");
    }

    private PreparedStatement createPreparedStatement(ConnectionSessionDTO dto, String sql, List<Parameter> params,
                                                      StatementRequest request)
            throws SQLException {
        log.info("Creating prepared statement for {}", sql);

        PreparedStatement ps = null;
        if (request.getProperties().isEmpty()) {
            ps = dto.getConnection().prepareStatement(sql);
        }
        Map<String, Object> properties = EMPTY_MAP;
        if (!request.getProperties().isEmpty()) {
            properties = deserialize(request.getProperties().toByteArray(), Map.class);
        }
        if (properties.isEmpty()) {
            ps = dto.getConnection().prepareStatement(sql);
        }
        if (properties.size() == 1) {
            int[] columnIndexes = (int[]) properties.get(CommonConstants.STATEMENT_COLUMN_INDEXES_KEY);
            String[] columnNames = (String[]) properties.get(CommonConstants.STATEMENT_COLUMN_INDEXES_KEY);
            Boolean isAddBatch = (Boolean) properties.get(CommonConstants.PREPARED_STATEMENT_ADD_BATCH_FLAG);
            if (columnIndexes != null) {
                ps = dto.getConnection().prepareStatement(sql, columnIndexes);
            } else if (columnNames != null) {
                ps = dto.getConnection().prepareStatement(sql, columnNames);
            } else if (isAddBatch) {
                ps = dto.getConnection().prepareStatement(sql);
            }
        }
        Integer resultSetType = (Integer) properties.get(CommonConstants.STATEMENT_RESULT_SET_TYPE_KEY);
        Integer resultSetConcurrency = (Integer) properties.get(CommonConstants.STATEMENT_RESULT_SET_CONCURRENCY_KEY);
        Integer resultSetHoldability = (Integer) properties.get(CommonConstants.STATEMENT_RESULT_SET_HOLDABILITY_KEY);

        if (resultSetType != null && resultSetConcurrency != null && resultSetHoldability == null) {
            ps = dto.getConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
        }
        if (resultSetType != null && resultSetConcurrency != null && resultSetHoldability != null) {
            ps = dto.getConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        if (ps == null) {
            throw new SQLException("Incorrect number of properties for creating a new prepared statement.");
        }

        this.addParametersPreparedStatement(dto, ps, params);
        return ps;
    }

    private void addParametersPreparedStatement(ConnectionSessionDTO dto, PreparedStatement ps, List<Parameter> params)
            throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Parameter parameter = params.get(i);
            this.addParam(dto.getSession(), parameter.getIndex(), ps, params.get(i));
        }
    }

    @Override
    public void executeQuery(StatementRequest request, StreamObserver<OpResult> responseObserver) {
        log.info("Executing query for {}", request.getSql());
        String stmtHash = SqlStatementXXHash.hashSqlQuery(request.getSql());
        try {
            circuitBreaker.preCheck(stmtHash);
            ConnectionSessionDTO dto = this.sessionConnection(request.getSession(), true);

            List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
            if (CollectionUtils.isNotEmpty(params)) {
                PreparedStatement ps = this.createPreparedStatement(dto, request.getSql(), params, request);
                String resultSetUUID = this.sessionManager.registerResultSet(dto.getSession(), ps.executeQuery());
                this.handleResultSet(dto.getSession(), resultSetUUID, responseObserver);
            } else {
                Statement stmt = this.createStatement(dto.getConnection(), request);
                String resultSetUUID = this.sessionManager.registerResultSet(dto.getSession(),
                        stmt.executeQuery(request.getSql()));
                this.handleResultSet(dto.getSession(), resultSetUUID, responseObserver);
            }
            circuitBreaker.onSuccess(stmtHash);
        } catch (SQLException e) {
            circuitBreaker.onFailure(stmtHash, e);
            log.error("Failure during query execution: " + e.getMessage(), e);
            sendSQLExceptionMetadata(e, responseObserver);
        }
    }

    @Override
    public StreamObserver<LobDataBlock> createLob(StreamObserver<LobReference> responseObserver) {
        log.info("Creating LOB");
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
                    log.info("lob data block received, lob type {}", this.lobType);
                    ConnectionSessionDTO dto = sessionConnection(lobDataBlock.getSession(), true);
                    Connection conn = dto.getConnection();
                    if (StringUtils.isEmpty(lobDataBlock.getSession().getSessionUUID()) || this.lobUUID == null) {
                        if (LobType.LT_BLOB.equals(this.lobType)) {
                            Blob newBlob = conn.createBlob();
                            this.lobUUID = UUID.randomUUID().toString();
                            sessionManager.registerLob(dto.getSession(), newBlob, this.lobUUID);
                        } else if (LobType.LT_CLOB.equals(this.lobType)) {
                            Clob newClob = conn.createClob();
                            this.lobUUID = UUID.randomUUID().toString();
                            sessionManager.registerLob(dto.getSession(), newClob, this.lobUUID);
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
                                //Need to first send the ref to the client before adding the stream as a parameter
                                sendLobRef(dto, lobDataBlock.getData().toByteArray().length);

                                //Add bite stream as parameter to the prepared statement
                                lobDataBlocksInputStream = new LobDataBlocksInputStream(lobDataBlock);
                                //Only needs to be registered so we can wait it to receive all bytes before performing the update.
                                sessionManager.registerLob(dto.getSession(), lobDataBlocksInputStream, lobDataBlocksInputStream.getUuid());
                                sessionManager.registerAttr(dto.getSession(), lobDataBlocksInputStream.getUuid(), metadata);
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
                log.info("Returning lob ref {}", this.lobUUID);
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
                log.error("Failure lob stream: " + throwable.getMessage(), throwable);
                if (lobDataBlocksInputStream != null) {
                    lobDataBlocksInputStream.finish(true);
                }
            }

            @Override
            public void onCompleted() {
                if (lobDataBlocksInputStream != null) {
                    CompletableFuture.runAsync(() -> {
                        log.info("Finishing lob stream for lob ref {}", this.lobUUID);
                        lobDataBlocksInputStream.finish(true);
                    });
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
        log.info("Reading lob {}", request.getLobReference().getUuid());
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
            boolean nextBlockFullyEmpty = false;
            while (nextByte != -1) {
                nextBlock[++idx] = (byte) nextByte;
                nextBlockFullyEmpty = false;
                if (idx == nextBlockSize - 1) {
                    currentPos += (idx + 1);
                    log.info("Sending block of data size {} pos {}", idx + 1, currentPos);
                    //Send data to client in limited size blocks to safeguard server memory.
                    responseObserver.onNext(LobDataBlock.newBuilder()
                            .setSession(lobRef.getSession())
                            .setPosition(currentPos)
                            .setData(ByteString.copyFrom(nextBlock))
                            .build()
                    );
                    nextBlockSize = this.nextBlockSize(readLobContext, currentPos - 1);
                    if (nextBlockSize > 0) {//Might be a single small block then nextBlockSize will return negative.
                        nextBlock = new byte[nextBlockSize];
                    } else {
                        nextBlock = new byte[0];
                    }
                    nextBlockFullyEmpty = true;
                    idx = -1;
                }
                nextByte = inputStream.read();
            }

            //Send leftover bytes
            if (!nextBlockFullyEmpty && nextBlock.length > 0 && nextBlock[0] != -1) {

                byte[] adjustedSizeArray = (idx % MAX_LOB_DATA_BLOCK_SIZE != 0 && !exactSizeKnown) ?
                        trim(nextBlock) : nextBlock;
                currentPos = (int) request.getPosition() + idx;
                log.info("Sending leftover bytes size {} pos {}", idx, currentPos);
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
            log.info("Terminating session");
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
        log.info("Starting transaction");
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
        log.info("Commiting transaction");
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
        log.info("Rollback transaction");
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

    @Override
    public void callResource(CallResourceRequest request, StreamObserver<CallResourceResponse> responseObserver) {
        try {
            if (!request.hasSession()) {
                throw new SQLException("No active session.");//TODO review might have to create a session in some cases
            }

            CallResourceResponse.Builder responseBuilder = CallResourceResponse.newBuilder();

            Object resource = switch (request.getResourceType()) {
                case RES_RESULT_SET -> sessionManager.getResultSet(request.getSession(), request.getResourceUUID());
                case RES_LOB -> sessionManager.getLob(request.getSession(), request.getResourceUUID());
                case RES_STATEMENT -> {
                    ConnectionSessionDTO csDto = sessionConnection(request.getSession(), true);
                    responseBuilder.setSession(csDto.getSession());
                    Statement statement = null;
                    if (!request.getResourceUUID().isBlank()) {
                        statement = sessionManager.getStatement(csDto.getSession(), request.getResourceUUID());
                    } else {
                        statement = csDto.getConnection().createStatement();
                        String uuid = sessionManager.registerStatement(csDto.getSession(), statement);
                        responseBuilder.setResourceUUID(uuid);
                    }
                    yield statement;
                }
                case RES_PREPARED_STATEMENT -> {
                    ConnectionSessionDTO csDto = sessionConnection(request.getSession(), true);
                    responseBuilder.setSession(csDto.getSession());
                    PreparedStatement ps = null;
                    if (!request.getResourceUUID().isBlank()) {
                        ps = sessionManager.getPreparedStatement(request.getSession(), request.getResourceUUID());
                    } else {
                        Map<String, Object> mapProperties = EMPTY_MAP;
                        if (!request.getProperties().isEmpty()) {
                            mapProperties = deserialize(request.getProperties().toByteArray(), Map.class);
                        }
                        ps = csDto.getConnection().prepareStatement((String) mapProperties.get(CommonConstants.PREPARED_STATEMENT_SQL_KEY));
                        String uuid = sessionManager.registerPreparedStatement(csDto.getSession(), ps);
                        responseBuilder.setResourceUUID(uuid);
                    }
                    yield ps;
                }
                case RES_CALLABLE_STATEMENT ->
                        sessionManager.getCallableStatement(request.getSession(), request.getResourceUUID());
                case RES_CONNECTION -> {
                    ConnectionSessionDTO csDto = sessionConnection(request.getSession(), true);
                    responseBuilder.setSession(csDto.getSession());
                    yield csDto.getConnection();
                }
                case RES_SAVEPOINT -> sessionManager.getAttr(request.getSession(), request.getResourceUUID());
                default -> throw new RuntimeException("Resource type invalid");
            };

            if (responseBuilder.getSession() == null || StringUtils.isBlank(responseBuilder.getSession().getSessionUUID())) {
                responseBuilder.setSession(request.getSession());
            }

            List<Object> paramsReceived = (request.getTarget().getParams().size() > 0) ?
                    deserialize(request.getTarget().getParams().toByteArray(), List.class) : EMPTY_LIST;
            Class<?> clazz = resource.getClass();
            if (CallType.CALL_RELEASE.equals(request.getTarget().getCallType()) &&
                "Savepoint".equalsIgnoreCase(request.getTarget().getResourceName())) {
                Savepoint savepoint = (Savepoint) this.sessionManager.getAttr(request.getSession(),
                        (String) paramsReceived.get(0));
                ((Connection) resource).releaseSavepoint(savepoint);
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
            Method method = this.findMethodByName(clazz, methodName(request.getTarget()), paramsReceived);
            java.lang.reflect.Parameter[] params = method.getParameters();
            Object resultFirstLevel = null;
            if (params != null && params.length > 0) {
                resultFirstLevel = method.invoke(resource, paramsReceived.toArray());
                if (resultFirstLevel instanceof CallableStatement cs) {
                    resultFirstLevel = this.sessionManager.registerCallableStatement(responseBuilder.getSession(), cs);
                }
            } else {
                resultFirstLevel = method.invoke(resource);
                if (resultFirstLevel instanceof Savepoint sp) {
                    String uuid = UUID.randomUUID().toString();
                    resultFirstLevel = uuid;
                    this.sessionManager.registerAttr(responseBuilder.getSession(), uuid, sp);
                } else if (resultFirstLevel instanceof ResultSet rs) {
                    resultFirstLevel = this.sessionManager.registerResultSet(responseBuilder.getSession(), rs);
                }
            }
            if (request.getTarget().hasNextCall()) {
                //Second level calls, for cases like getMetadata().isAutoIncrement(int column)
                Class<?> clazzNext = resultFirstLevel.getClass();
                List<Object> paramsReceived2 = (request.getTarget().getNextCall().getParams().size() > 0) ?
                        deserialize(request.getTarget().getNextCall().getParams().toByteArray(), List.class) :
                        EMPTY_LIST;
                Method methodNext = this.findMethodByName(clazzNext, methodName(request.getTarget().getNextCall()),
                        paramsReceived2);
                params = methodNext.getParameters();
                Object resultSecondLevel = null;
                if (params != null && params.length > 0) {
                    resultSecondLevel = methodNext.invoke(resultFirstLevel, paramsReceived2.toArray());
                } else {
                    resultSecondLevel = methodNext.invoke(resultFirstLevel);
                }
                if (resultSecondLevel instanceof ResultSet rs) {
                    resultSecondLevel = this.sessionManager.registerResultSet(responseBuilder.getSession(), rs);
                }
                responseBuilder.setValues(ByteString.copyFrom(serialize(resultSecondLevel)));
            } else {
                responseBuilder.setValues(ByteString.copyFrom(serialize(resultFirstLevel)));
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (SQLException se) {
            sendSQLExceptionMetadata(se, responseObserver);
        } catch (Exception e) {
            sendSQLExceptionMetadata(new SQLException("Unable to call resource: " + e.getMessage()), responseObserver);
        }
    }

    private Method findMethodByName(Class<?> clazz, String methodName, List<Object> params) {
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (methodName.equalsIgnoreCase(method.getName())) {
                if (method.getParameters().length == params.size()) {
                    boolean paramTypesMatch = true;
                    for (int i = 0; i < params.size(); i++) {
                        java.lang.reflect.Parameter reflectParam = method.getParameters()[i];
                        Object receivedParam = params.get(i);
                        //TODO there is a potential issue here, if parameters are received null and more than one method recives the same amount of parameters there is no way to distinguish. Maybe send a Null object with the class type as an attribute and parse it back to null in server is a solution.
                        Class reflectType = this.getWrapperType(reflectParam.getType());
                        if (receivedParam != null && (!reflectType.equals(receivedParam.getClass()) &&
                                !reflectType.isAssignableFrom(receivedParam.getClass()))) {
                            paramTypesMatch = false;
                            break;
                        }
                    }
                    if (paramTypesMatch) {
                        return method;
                    }
                }
            }
        }
        throw new RuntimeException("Method " + methodName + " not found in class " + clazz.getName());
    }

    // Helper method to get the wrapper class for a primitive type
    private Class<?> getWrapperType(Class<?> primitiveType) {
        if (primitiveType == int.class) return Integer.class;
        if (primitiveType == boolean.class) return Boolean.class;
        if (primitiveType == byte.class) return Byte.class;
        if (primitiveType == char.class) return Character.class;
        if (primitiveType == double.class) return Double.class;
        if (primitiveType == float.class) return Float.class;
        if (primitiveType == long.class) return Long.class;
        if (primitiveType == short.class) return Short.class;
        return primitiveType; // for non-primitives
    }

    private String methodName(TargetCall target) throws SQLException {
        String prefix = switch (target.getCallType()) {
            case CALL_IS -> "is";
            case CALL_GET -> "get";
            case CALL_SET -> "set";
            case CALL_ALL -> "all";
            case CALL_NULLS -> "nulls";
            case CALL_USES -> "uses";
            case CALL_SUPPORTS -> "supports";
            case CALL_STORES -> "stores";
            case CALL_NULL -> "null";
            case CALL_DOES -> "does";
            case CALL_DATA -> "data";
            case CALL_NEXT -> "next";
            case CALL_CLOSE -> "close";
            case CALL_WAS -> "was";
            case CALL_CLEAR -> "clear";
            case CALL_FIND -> "find";
            case CALL_BEFORE -> "before";
            case CALL_AFTER -> "after";
            case CALL_FIRST -> "first";
            case CALL_LAST -> "last";
            case CALL_ABSOLUTE -> "absolute";
            case CALL_RELATIVE -> "relative";
            case CALL_PREVIOUS -> "previous";
            case CALL_ROW -> "row";
            case CALL_UPDATE -> "update";
            case CALL_INSERT -> "insert";
            case CALL_DELETE -> "delete";
            case CALL_REFRESH -> "refresh";
            case CALL_CANCEL -> "cancel";
            case CALL_MOVE -> "move";
            case CALL_OWN -> "own";
            case CALL_OTHERS -> "others";
            case CALL_UPDATES -> "updates";
            case CALL_DELETES -> "deletes";
            case CALL_INSERTS -> "inserts";
            case CALL_LOCATORS -> "locators";
            case CALL_AUTO -> "auto";
            case CALL_GENERATED -> "generated";
            case CALL_RELEASE -> "release";
            case CALL_NATIVE -> "native";
            case CALL_PREPARE -> "prepare";
            case CALL_ROLLBACK -> "rollback";
            case CALL_ABORT -> "abort";
            case CALL_EXECUTE -> "execute";
            case CALL_ADD -> "add";
            case CALL_ENQUOTE -> "enquote";
            case UNRECOGNIZED -> throw new SQLException("CALL type not supported.");
        };
        return prefix + target.getResourceName();
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
            //log.info("Lookup connection hash -> " + sessionInfo.getConnHash());
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
                        currentValue = UUID.randomUUID().toString();
                        this.sessionManager.registerLob(session, blob, currentValue.toString());
                    }
                    case Types.CLOB -> {
                        Clob clob = rs.getClob(i + 1);
                        currentValue = UUID.randomUUID().toString();
                        this.sessionManager.registerLob(session, clob, currentValue.toString());
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
                            currentValue = UUID.randomUUID().toString();
                            this.sessionManager.registerLob(session, inputStream, currentValue.toString());
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
        log.info("Adding parameter idx {} type {}", idx, param.getType().toString());
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
        return url.replaceAll(CommonConstants.OJP_REGEX_PATTERN + "_", EMPTY_STRING);
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