package org.openjdbcproxy.grpc.server;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.OpContext;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.ResultSetId;
import com.openjdbcproxy.grpc.ResultType;
import com.openjdbcproxy.grpc.StatementRequest;
import com.openjdbcproxy.grpc.StatementServiceGrpc;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.stub.StreamObserver;
import org.apache.commons.collections4.CollectionUtils;
import org.openjdbcproxy.constants.CommonConstants;
import org.openjdbcproxy.grpc.dto.OpQueryResult;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;
import static org.openjdbcproxy.grpc.server.Constants.EMPTY_STRING;
import static org.openjdbcproxy.grpc.server.Constants.H2_DRIVER_CLASS;
import static org.openjdbcproxy.grpc.server.Constants.OJP_DRIVER_PREFIX;
import static org.openjdbcproxy.grpc.server.Constants.SHA_256;
import static org.openjdbcproxy.grpc.server.GrpcExceptionHandler.sendSQLExceptionMetadata;

public class StatementServiceImpl extends StatementServiceGrpc.StatementServiceImplBase {

    //TODO put the datasource at database level not user + database so if more than one user agaist the DB still maintain the max pool size
    private final Map<String, HikariDataSource> datasourceMap = new ConcurrentHashMap<>();
    private final Map<String, ResultSet> resultSetMap = new ConcurrentHashMap<>();


    static {
        //TODO register all JDBC drivers supported here.
        try {
            Class.forName(H2_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void connect(ConnectionDetails connectionDetails, StreamObserver<OpContext> responseObserver) {
        String connHash = hashConnectionDetails(connectionDetails);

        HikariDataSource ds = this.datasourceMap.get(connHash);
        if (ds == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl( this.parseUrl(connectionDetails.getUrl()) );
            config.setUsername( connectionDetails.getUser() );
            config.setPassword( connectionDetails.getPassword() );
            config.addDataSourceProperty( "cachePrepStmts" , "true" );
            config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
            config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
            config.addDataSourceProperty( "maximumPoolSize" , 1 );
            config.addDataSourceProperty( "minimumPoolSize" , 1 );
            ds = new HikariDataSource( config );

            this.datasourceMap.put(connHash, ds);
        }

        responseObserver.onNext(OpContext.newBuilder().setConnHash(connHash).build());
        responseObserver.onCompleted();
    }

    @Override
    public void executeUpdate(StatementRequest request, StreamObserver<OpResult> responseObserver) {
        Integer updated = 0;

        try (Connection conn = this.datasourceMap.get(request.getContext().getConnHash()).getConnection()) {
            List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
            if (CollectionUtils.isNotEmpty(params)) {
                try (PreparedStatement ps = conn.prepareStatement(request.getSql())) {
                    for (int i = 0; i < params.size(); i++) {
                        this.addParam(i + 1, ps, params.get(i));
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
        }

        responseObserver.onNext(OpResult.newBuilder().setType(ResultType.INTEGER).setValue(
                ByteString.copyFrom(serialize(updated))).build());
        responseObserver.onCompleted();
    }

    @Override
    public void executeQuery(StatementRequest request, StreamObserver<OpResult> responseObserver) {
        OpResult.Builder resultsBuilder = OpResult.newBuilder();

        try {
            Connection conn = this.datasourceMap.get(request.getContext().getConnHash()).getConnection();

            List<Parameter> params = deserialize(request.getParameters().toByteArray(), List.class);
            if (CollectionUtils.isNotEmpty(params)) {
                PreparedStatement ps = conn.prepareStatement(request.getSql());
                for (int i = 0; i < params.size(); i++) {
                    this.addParam(i + 1, ps, params.get(i));
                }
                String resultSetUUID = this.registerResultSet(ps.executeQuery());
                this.handleResultSet(resultSetUUID, resultsBuilder, true);
            } else {
                Statement stmt = conn.createStatement();
                String resultSetUUID = this.registerResultSet(stmt.executeQuery(request.getSql()));
                this.handleResultSet(resultSetUUID, resultsBuilder, true);
            }

        } catch (SQLException e) {
            sendSQLExceptionMetadata(e, responseObserver);
        }

        resultsBuilder.setType(ResultType.RESULT_SET);
        responseObserver.onNext(resultsBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void readResultSetData(ResultSetId resultSetId, StreamObserver<OpResult> responseObserver) {
        OpResult.Builder resultsBuilder = OpResult.newBuilder();
        try {
            this.handleResultSet(resultSetId.getUuid(), resultsBuilder, false);
        } catch (SQLException e) {
            sendSQLExceptionMetadata(e, responseObserver);
        }

        resultsBuilder.setType(ResultType.RESULT_SET);
        responseObserver.onNext(resultsBuilder.build());
        responseObserver.onCompleted();
    }

    private String registerResultSet(ResultSet rs) throws SQLException {
        String resultSetUUID = UUID.randomUUID().toString();
        this.resultSetMap.put(resultSetUUID, rs);//TODO prevent other clients from accessing random result sets, currently there is not protection
        return resultSetUUID;
    }

    private void handleResultSet(String resultSetUUID, OpResult.Builder resultsBuilder, boolean firstRead) throws SQLException {
        ResultSet rs = this.resultSetMap.get(resultSetUUID);
        OpQueryResult.OpQueryResultBuilder queryResultBuilder = OpQueryResult.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        if (firstRead) {
            List<String> labels = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                labels.add(rs.getMetaData().getColumnName(i + 1));
            }
            queryResultBuilder.labels(labels);
        }
        List<Object[]> results = new ArrayList<>();
        int row = 0;
        while (row < CommonConstants.ROWS_PER_RESULT_SET_DATA_BLOCK && rs.next()) {
            row++;
            Object[] rowValues = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                rowValues[i] = rs.getObject(i + 1);
            }
            results.add(rowValues);
        }
        boolean moreData = row >= CommonConstants.ROWS_PER_RESULT_SET_DATA_BLOCK;
        queryResultBuilder.resultSetUUID(resultSetUUID);
        queryResultBuilder.moreData(moreData);
        queryResultBuilder.rows(results);
        resultsBuilder.setValue(ByteString.copyFrom(serialize(queryResultBuilder.build())));
        if (!moreData) {// TODO close should come from the client when the client close these objects.
            rs.close();
            rs.getStatement().close();
            rs.getStatement().getConnection().close();
        }
    }

    private void addParam(int idx, PreparedStatement ps, Parameter param) throws SQLException {
        switch (param.getType()) {
            case INT -> ps.setInt(idx, (int) param.getValues().getFirst());
            case DOUBLE -> ps.setDouble(idx, (double) param.getValues().getFirst());
            case STRING -> ps.setString(idx, (String) param.getValues().getFirst());
            case LONG -> ps.setLong(idx, (long) param.getValues().getFirst());
            case BOOLEAN -> ps.setBoolean(idx, (boolean) param.getValues().getFirst());
            case BIG_DECIMAL -> ps.setBigDecimal(idx, (BigDecimal) param.getValues().getFirst());
            case FLOAT -> ps.setFloat(idx, (float) param.getValues().getFirst());
            case BYTES -> ps.setBytes(idx, (byte[]) param.getValues().getFirst());
            case BYTE -> ps.setByte(idx, ((byte[]) param.getValues().getFirst())[0]);//Comes as an array of bytes with one element.
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