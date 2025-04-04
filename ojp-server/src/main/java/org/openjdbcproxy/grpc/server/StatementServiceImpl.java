package org.openjdbcproxy.grpc.server;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.ReadLobRequest;
import com.openjdbcproxy.grpc.ResultType;
import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.SessionTerminationStatus;
import com.openjdbcproxy.grpc.StatementRequest;
import com.openjdbcproxy.grpc.StatementServiceGrpc;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.openjdbcproxy.constants.CommonConstants;
import org.openjdbcproxy.grpc.dto.OpQueryResult;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.io.BufferedInputStream;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;
import static org.openjdbcproxy.grpc.server.Constants.EMPTY_STRING;
import static org.openjdbcproxy.grpc.server.Constants.H2_DRIVER_CLASS;
import static org.openjdbcproxy.grpc.server.Constants.OJP_DRIVER_PREFIX;
import static org.openjdbcproxy.grpc.server.Constants.SHA_256;
import static org.openjdbcproxy.grpc.server.GrpcExceptionHandler.sendSQLExceptionMetadata;
import static org.openjdbcproxy.constants.CommonConstants.MAX_LOB_DATA_BLOCK_SIZE;

@RequiredArgsConstructor
public class StatementServiceImpl extends StatementServiceGrpc.StatementServiceImplBase {

    //TODO put the datasource at database level not user + database so if more than one user agaist the DB still maintain the max pool size
    private final Map<String, HikariDataSource> datasourceMap = new ConcurrentHashMap<>();
    //private final Map<String, ResultSet> resultSetMap = new ConcurrentHashMap<>();
    //private final Map<String, Connection> connectionMap = new ConcurrentHashMap<>();
    //private final Map<String, Blob> blobMap = new ConcurrentHashMap<>();
    private final SessionManager sessionManager;

    static {
        //TODO register all JDBC drivers supported here.
        try {
            Class.forName(H2_DRIVER_CLASS);
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
                if (CollectionUtils.isNotEmpty(params)) {
                    PreparedStatement ps = dto.getConnection().prepareStatement(request.getSql());
                    for (int i = 0; i < params.size(); i++) {
                        this.addParam(dto.getSession(), i + 1, ps, params.get(i));
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
                if (StringUtils.isEmpty(dto.getSession().getSessionUUID())) {
                    assert stmt != null;
                    stmt.close();
                    dto.getConnection().close();
                } else {
                    returnSessionInfo = dto.getSession();
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

    @Override
    public void executeQuery(StatementRequest request, StreamObserver<OpResult> responseObserver) {

        try {
            ConnectionSessionDTO dto = this.sessionConnection(request.getSession(), true);

            List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
            if (CollectionUtils.isNotEmpty(params)) {
                PreparedStatement ps = dto.getConnection().prepareStatement(request.getSql());
                for (int i = 0; i < params.size(); i++) {
                    this.addParam(dto.getSession(), i + 1, ps, params.get(i));
                }
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
                    ConnectionSessionDTO dto = sessionConnection(lobDataBlock.getSession(), true);
                    Connection conn = dto.getConnection();
                    if (StringUtils.isEmpty(lobDataBlock.getSession().getSessionUUID()) || this.lobUUID == null) {
                        Blob newBlob = conn.createBlob();
                        this.lobUUID = sessionManager.registerBlob(dto.getSession(), newBlob);
                    }

                    Blob blob = sessionManager.getBlob(dto.getSession(), this.lobUUID);
                    byte[] byteArrayData = lobDataBlock.getData().toByteArray();
                    int bytesWritten = blob.setBytes(lobDataBlock.getPosition(), byteArrayData);
                    this.countBytesWritten.addAndGet(bytesWritten);
                    this.sessionInfo = dto.getSession();

                    if (isFirstBlock.get()) {
                        //Send one flag response to indicate that the Blob has been created successfully and the first
                        // block fo data has been written successfully.
                        responseObserver.onNext(LobReference.newBuilder()
                                .setSession(dto.getSession())
                                .setUuid(this.lobUUID)
                                .setBytesWritten(bytesWritten)
                                .build()
                        );
                        isFirstBlock.set(false);
                    }

                } catch (SQLException e) {
                    sendSQLExceptionMetadata(e, responseObserver);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                //TODO see what to do.
            }

            @Override
            public void onCompleted() {
                //Send the final Lob reference with total count of written bytes.
                responseObserver.onNext(LobReference.newBuilder()
                        .setSession(this.sessionInfo)
                        .setUuid(this.lobUUID)
                        .setBytesWritten(this.countBytesWritten.get())
                        .build()
                );responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void readLob(ReadLobRequest request, StreamObserver<LobDataBlock> responseObserver) {
        try {
            LobReference lobRef = request.getLobReference();
            Blob blob = this.sessionManager.getBlob(lobRef.getSession(), lobRef.getUuid());
            long blobLength = blob.length();
            long readLength = (request.getLength() < blobLength) ? request.getLength() : blobLength;
            BufferedInputStream bisBlob = new BufferedInputStream(blob.getBinaryStream(request.getPosition(), readLength));
            byte[] nextBlobk = bisBlob.readNBytes(MAX_LOB_DATA_BLOCK_SIZE);
            int blockCount = 0;
            while (nextBlobk.length > 0) {
                //Send data to client in limited size blocks to safeguard server memory.
                responseObserver.onNext(LobDataBlock.newBuilder()
                        .setSession(lobRef.getSession())
                        .setPosition(((long) blockCount * MAX_LOB_DATA_BLOCK_SIZE) + nextBlobk.length + 1)
                        .setData(ByteString.copyFrom(nextBlobk))
                        .build()
                );
                blockCount++;
                nextBlobk = bisBlob.readNBytes(MAX_LOB_DATA_BLOCK_SIZE);
            }
            responseObserver.onCompleted();

        } catch (SQLException se) {
            sendSQLExceptionMetadata(se, responseObserver);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            throw new RuntimeException(e);
        }
    }

    /**
     * Finds a suitable connection for the current sessionInfo.
     * If there is a connection already in the sessionInfo reuse it, if not ge a fresh one from the data source.
     *
     * @param sessionInfo - current sessionInfo object.
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
                Object currentValue = rs.getObject(i + 1);
                if (currentValue instanceof Blob blob) {
                    String newBlobUUID = this.sessionManager.registerBlob(session, blob);
                    currentValue = newBlobUUID;
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
            case BLOB -> ps.setBlob(idx, this.sessionManager.getBlob(session, (String) param.getValues().getFirst()));
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