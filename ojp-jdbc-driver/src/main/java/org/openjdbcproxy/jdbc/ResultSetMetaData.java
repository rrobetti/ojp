package org.openjdbcproxy.jdbc;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
import com.openjdbcproxy.grpc.ResourceType;
import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.TargetCall;
import org.openjdbcproxy.grpc.client.StatementService;

import java.sql.SQLException;
import java.util.List;

import static org.openjdbcproxy.grpc.SerializationHandler.serialize;
import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;

public class ResultSetMetaData implements java.sql.ResultSetMetaData {
    private final SessionInfo session;
    private final String resultSetUUID;
    private final StatementService statementService;
    private final ResultSet resultSet;

    public ResultSetMetaData(SessionInfo session, String resultSetUUID, ResultSet resultSet, StatementService statementService) {
        this.session = session;
        this.resultSetUUID = resultSetUUID;
        this.resultSet = resultSet;
        this.statementService = statementService;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return resultSet.getLabelsMap().size();
    }

    private CallResourceRequest.Builder newCallBuilder() {
        return CallResourceRequest.newBuilder()
                .setSession(this.session)
                .setResourceType(ResourceType.RES_RESULT_SET)
                .setResourceUUID(this.resultSetUUID);
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "AutoIncrement", column, Boolean.class);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "CaseSensitive", column, Boolean.class);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "Searchable", column, Boolean.class);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "Currency", column, Boolean.class);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "Nullable", column, Integer.class);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "Signed", column, Boolean.class);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ColumnDisplaySize", column, Integer.class);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ColumnLabel", column, String.class);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ColumnName", column, String.class);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "SchemaName", column, String.class);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "Precision", column, Integer.class);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "Scale", column, Integer.class);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "TableName", column, String.class);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "CatalogName", column, String.class);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ColumnType", column, Integer.class);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ColumnTypeName", column, String.class);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "ReadOnly", column, Boolean.class);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "Writable", column, Boolean.class);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "DefinitelyWritable", column, Boolean.class);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ColumnClassName", column, String.class);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Unwrap not supported.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("isWrappedFor not supported.");
    }

    private <T> T retrieveMetadataAttribute(CallType callType, String attrName, Integer column,  Class returnType) throws SQLException {
        CallResourceRequest.Builder reqBuilder = this.newCallBuilder();
        List<Object> params = List.of(Integer.valueOf(column));
        reqBuilder.setTarget(
                TargetCall.newBuilder()
                        .setCallType(CallType.CALL_GET)
                        .setResourceName("MetaData")
                        .setNextCall(TargetCall.newBuilder()
                                .setCallType(callType)
                                .setResourceName(attrName)
                                .setParams(ByteString.copyFrom(serialize(params)))
                                .build())
                        .build()
        );
        CallResourceResponse response = this.statementService.callResource(reqBuilder.build());
        return (T) deserialize(response.getValues().toByteArray(), returnType);
    }
}
