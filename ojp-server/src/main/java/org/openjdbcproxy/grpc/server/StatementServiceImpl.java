package org.openjdbcproxy.grpc.server;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.ReadLobRequest;
import com.openjdbcproxy.grpc.ResultType;
import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.StatementRequest;
import com.openjdbcproxy.grpc.StatementServiceGrpc;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
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

public class StatementServiceImpl extends StatementServiceGrpc.StatementServiceImplBase {

    //TODO put the datasource at database level not user + database so if more than one user agaist the DB still maintain the max pool size
    private final Map<String, HikariDataSource> datasourceMap = new ConcurrentHashMap<>();
    private final Map<String, ResultSet> resultSetMap = new ConcurrentHashMap<>();
    private final Map<String, Connection> connectionMap = new ConcurrentHashMap<>();
    private final Map<String, Blob> blobMap = new ConcurrentHashMap<>();

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

        try {
            Connection conn = findSuitableConnection(request.getSession(), false).getConnection();
            try {
                List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
                if (CollectionUtils.isNotEmpty(params)) {
                    try (PreparedStatement ps = conn.prepareStatement(request.getSql())) {
                        for (int i = 0; i < params.size(); i++) {
                            this.addParam(request.getSession(), i + 1, ps, params.get(i));
                        }
                        updated = ps.executeUpdate();
                    }
                } else {
                    try (Statement stmt = conn.createStatement()) {
                        updated = stmt.executeUpdate(request.getSql());
                    }
                }
            } catch (SQLException e) {
                sendSQLExceptionMetadata(e, responseObserver);
            } finally {
                if (StringUtils.isEmpty(request.getSession().getConnectionUUID())) {
                    conn.close();
                }
            }
        } catch (SQLException e) {// Need a second catch just for the acquisition of the connection
            sendSQLExceptionMetadata(e, responseObserver);
        }

        responseObserver.onNext(OpResult.newBuilder().setType(ResultType.INTEGER).setValue(
                ByteString.copyFrom(serialize(updated))).build());
        responseObserver.onCompleted();
    }

    @Override
    public void executeQuery(StatementRequest request, StreamObserver<OpResult> responseObserver) {

        try {
            Connection conn = this.findSuitableConnection(request.getSession(), false).getConnection();

            List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
            if (CollectionUtils.isNotEmpty(params)) {
                PreparedStatement ps = conn.prepareStatement(request.getSql());
                for (int i = 0; i < params.size(); i++) {
                    this.addParam(request.getSession(), i + 1, ps, params.get(i));
                }
                String resultSetUUID = this.registerResultSet(request.getSession(), ps.executeQuery());
                this.handleResultSet(request.getSession(), resultSetUUID, responseObserver);
            } else {
                Statement stmt = conn.createStatement();
                String resultSetUUID = this.registerResultSet(request.getSession(), stmt.executeQuery(request.getSql()));
                this.handleResultSet(request.getSession(), resultSetUUID, responseObserver);
            }

        } catch (SQLException e) {
            sendSQLExceptionMetadata(e, responseObserver);
        }
    }

    @Override
    public StreamObserver<LobDataBlock> createLob(StreamObserver<LobReference> responseObserver) {

        return new ServerCallStreamObserver<>() {
            private SessionInfo session;
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
                    ConnectionSessionDTO dto = findSuitableConnection(lobDataBlock.getSession(), true);
                    Connection conn = dto.getConnection();
                    if (this.session == null || this.session.getConnectionUUID() == null) {
                        this.session = dto.getSession();
                        Blob newBlob = conn.createBlob();
                        if (this.lobUUID == null) {
                            this.lobUUID = UUID.randomUUID().toString();
                        }
                        blobMap.put(composedKey(this.session) + this.lobUUID, newBlob);
                    }

                    Blob blob = blobMap.get(composedKey(this.session) + this.lobUUID);
                    byte[] byteArrayData = lobDataBlock.getData().toByteArray();
                    int bytesWritten = blob.setBytes(lobDataBlock.getPosition(), byteArrayData);
                    this.countBytesWritten.addAndGet(bytesWritten);

                    if (isFirstBlock.get()) {
                        //Send one flag response to indicate that the Blob has been created successfully and the first
                        // block fo data has been written successfully.
                        responseObserver.onNext(LobReference.newBuilder()
                                .setSession(this.session)
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
                        .setSession(this.session)
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
            Blob blob = this.blobMap.get(this.composedKey(lobRef.getSession()) + lobRef.getUuid());
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
            //Once all data is sent there is no use to keep the blob.
            this.blobMap.remove(this.composedKey(lobRef.getSession()) + lobRef.getUuid());
            responseObserver.onCompleted();

        } catch (SQLException se) {
            sendSQLExceptionMetadata(se, responseObserver);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a composed key formed by the connection hash + client uuid + connection uuid (if) present.
     * The result is used as prefix for keys in the maps of ReusltSets, Connections, Blobs etc so that one client
     * cannot access another client object just by having the id of the object.
     * @param session SessionInfo
     * @return String composed key.
     */
    private String composedKey(SessionInfo session) {
        return session.getConnHash() + session.getClientUUID() +
                (session.getConnectionUUID() != null ? session.getConnectionUUID() : "");
    }

    /**
     * Finds a suitable connection for the current session.
     * If there is a connection already in the session reuse it, if not ge a fresh one from the data source.
     *
     * @param session - current session object.
     * @param startSessionIfNone - if true will start a new session if none exists.
     * @return ConnectionSessionDTO
     * @throws SQLException if connection not found or closed (by timeout or other reason)
     */
    private ConnectionSessionDTO findSuitableConnection(SessionInfo session, boolean startSessionIfNone) throws SQLException {
        ConnectionSessionDTO.ConnectionSessionDTOBuilder dtoBuilder = ConnectionSessionDTO.builder();
        dtoBuilder.session(session);
        Connection conn;
        if (StringUtils.isNotEmpty(session.getConnectionUUID())) {
            conn = this.connectionMap.get(this.composedKey(session));
            if (conn == null) {
                throw new SQLException("Connection not found for this session");
            }
            if (conn.isClosed()) {
                throw new SQLException("Connection is closed");
            }
        } else {
            conn = this.datasourceMap.get(session.getConnHash()).getConnection();
            if (startSessionIfNone) {
                String newConnectionUUID = UUID.randomUUID().toString();
                SessionInfo updatedSession = SessionInfo.newBuilder()
                        .setConnHash(session.getConnHash())
                        .setClientUUID(session.getClientUUID())
                        .setConnectionUUID(newConnectionUUID)
                        .build();
                this.connectionMap.put(composedKey(updatedSession), conn);
                dtoBuilder.session(updatedSession);
            }
        }
        dtoBuilder.connection(conn);

        return dtoBuilder.build();
    }

    private String registerResultSet(SessionInfo session, ResultSet rs) throws SQLException {
        String resultSetUUID = UUID.randomUUID().toString();
        this.resultSetMap.put(this.composedKey(session) + resultSetUUID, rs);//TODO prevent other clients from accessing random result sets, currently there is not protection
        return resultSetUUID;
    }

    private void handleResultSet(SessionInfo session, String resultSetUUID, StreamObserver<OpResult> responseObserver)
            throws SQLException {
        ResultSet rs = this.resultSetMap.get(this.composedKey(session) + resultSetUUID);
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
                    String newBlobUUID = UUID.randomUUID().toString();
                    this.blobMap.put(this.composedKey(session) + newBlobUUID, blob);
                    currentValue = newBlobUUID;
                }
                rowValues[i] = currentValue;
            }
            results.add(rowValues);

            if (row % CommonConstants.ROWS_PER_RESULT_SET_DATA_BLOCK == 0) {
                justSent = true;
                //Send a block of records
                responseObserver.onNext(this.wrapResults(results, queryResultBuilder, resultSetUUID));
                queryResultBuilder = OpQueryResult.builder();// Recreate the builder to not send labels in every block.
                results = new ArrayList<>();
            }
        }

        if (!justSent) {
            //Send a block of remaining records
            responseObserver.onNext(this.wrapResults(results, queryResultBuilder, resultSetUUID));
        }

        responseObserver.onCompleted();

        // TODO close should come from the client when the client close these objects. Maybe will have them in the session. One session migh have zero or more statements, prepared statemets and or resultsets
        rs.close();
        rs.getStatement().close();
        if (session.getConnectionUUID() == null) {
            rs.getStatement().getConnection().close();
        }
    }

    private OpResult wrapResults(List<Object[]> results, OpQueryResult.OpQueryResultBuilder queryResultBuilder,
                                 String resultSetUUID) {
        OpResult.Builder resultsBuilder = OpResult.newBuilder();
        resultsBuilder.setType(ResultType.RESULT_SET);
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
            case BLOB -> ps.setBlob(idx, this.blobMap.get(this.composedKey(session) + param.getValues().getFirst()));
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