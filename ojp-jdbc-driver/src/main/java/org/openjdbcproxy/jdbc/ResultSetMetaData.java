package org.openjdbcproxy.jdbc;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
import com.openjdbcproxy.grpc.ResourceType;
import com.openjdbcproxy.grpc.TargetCall;
import org.apache.commons.lang3.StringUtils;
import org.openjdbcproxy.grpc.client.StatementService;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;

public class ResultSetMetaData implements java.sql.ResultSetMetaData {
    private final StatementService statementService;
    private final RemoteProxyResultSet resultSet;
    private final PreparedStatement ps;

    public ResultSetMetaData(RemoteProxyResultSet resultSet, StatementService statementService) {
        this.resultSet = resultSet;
        this.statementService = statementService;
        this.ps = null;
    }

    public ResultSetMetaData(PreparedStatement ps, StatementService statementService) {
        this.ps = ps;
        this.statementService = statementService;
        this.resultSet = null;
    }

    @Override
    public int getColumnCount() throws SQLException {
        if (resultSet instanceof org.openjdbcproxy.jdbc.ResultSet rs) {
            return rs.getLabelsMap().size();
        } else {
            return this.retrieveMetadataAttribute(CallType.CALL_GET, "ColumnCount",-1, Integer.class);
        }
    }

    private CallResourceRequest.Builder newCallBuilder() throws SQLException {
        if (this.resultSet != null) {
            return CallResourceRequest.newBuilder()
                    .setSession(this.resultSet.getConnection().getSession())
                    .setResourceType(ResourceType.RES_RESULT_SET)
                    .setResourceUUID(this.resultSet.getResultSetUUID());
        } else if (this.ps != null) {
            CallResourceRequest.Builder builder = CallResourceRequest.newBuilder()
                    .setSession(this.ps.getConnection().getSession())
                    .setResourceType(ResourceType.RES_PREPARED_STATEMENT)
                    .setProperties(ByteString.copyFrom(serialize(this.ps.getProperties())));
            if (StringUtils.isNotBlank(this.ps.getPrepareStatementUUID())) {
                    builder.setResourceUUID(this.ps.getPrepareStatementUUID());
            }
            return builder;
        }
        throw new RuntimeException("A result set or a prepared statement reference is required.");
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
        List<Object> params = Constants.EMPTY_OBJECT_LIST;
        if (column > -1) {
            params = Arrays.asList(Integer.valueOf(column));
        }
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
        if (this.resultSet !=null) {
            this.resultSet.getConnection().setSession(response.getSession());
        } else if (this.ps != null) {
            this.ps.getConnection().setSession(response.getSession());
        }
        return (T) deserialize(response.getValues().toByteArray(), returnType);
    }
}
