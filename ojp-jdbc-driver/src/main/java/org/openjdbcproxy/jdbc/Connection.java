package org.openjdbcproxy.jdbc;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
import com.openjdbcproxy.grpc.ResourceType;
import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.TargetCall;
import com.openjdbcproxy.grpc.TransactionStatus;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.openjdbcproxy.constants.CommonConstants;
import org.openjdbcproxy.grpc.client.StatementService;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;

public class Connection implements java.sql.Connection {

    @Getter
    @Setter
    private SessionInfo session;
    private final StatementService statementService;
    private boolean autoCommit = true;
    private boolean readOnly = false;
    private boolean closed;

    public Connection(SessionInfo session, StatementService statementService) {
        this.session = session;
        this.statementService = statementService;
        this.closed = false;
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException {
        return new Statement(this, statementService);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
        return new PreparedStatement(this, sql, this.statementService);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        String remoteCallableStatementUUID = this.callProxy(CallType.CALL_PREPARE, "Call", String.class, Arrays.asList(sql));
        return new org.openjdbcproxy.jdbc.CallableStatement(this, this.statementService, remoteCallableStatementUUID);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return this.callProxy(CallType.CALL_NATIVE, "SQL", String.class, Arrays.asList(sql));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        //if switching on autocommit with active transaction, commit current transaction.
        if (!this.autoCommit && autoCommit &&
                TransactionStatus.TRX_ACTIVE.equals(session.getTransactionInfo().getTransactionStatus())) {
            this.session = this.statementService.commitTransaction(this.session);
            //If switching autocommit off, start a new transaction
        } else if (this.autoCommit && !autoCommit) {
            this.session = this.statementService.startTransaction(this.session);
        }

        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        if (!this.autoCommit) {
            this.session = this.statementService.commitTransaction(this.session);
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (!this.autoCommit) {
            this.session = this.statementService.rollbackTransaction(this.session);
        }
    }

    /**
     * Sends a signal to terminate the current session if one exist. It DOES NOT close a connection!
     * It is important to notice that if the system is using a connection pool, this method will not be actually called
     * very often and the termination of the session will relly on the SessionTerminationTrigger logic instead.
     *
     * @throws SQLException
     */
    @Override
    public void close() throws SQLException {
        if (StringUtils.isNotEmpty(this.session.getSessionUUID())) {
            this.statementService.terminateSession(this.session);
            this.session = null;
        }
        this.closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new org.openjdbcproxy.jdbc.DatabaseMetaData(this.session, this.statementService, this, null);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!DbInfo.isH2DB()) {
            this.readOnly = readOnly;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Catalog", Void.class, Arrays.asList(catalog));
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Catalog", String.class);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.callProxy(CallType.CALL_SET, "TransactionIsolation", Void.class, Arrays.asList(level));
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "TransactionIsolation", Integer.class);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Warnings", SQLWarning.class);
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new Statement(this, statementService, this.hashMapOf(
                List.of(
                        CommonConstants.STATEMENT_RESULT_SET_TYPE_KEY,
                        CommonConstants.STATEMENT_RESULT_SET_CONCURRENCY_KEY
                ), List.of(resultSetType, resultSetConcurrency)
        ));
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

        return new PreparedStatement(this, sql, statementService, this.hashMapOf(
                List.of(
                        CommonConstants.STATEMENT_RESULT_SET_TYPE_KEY,
                        CommonConstants.STATEMENT_RESULT_SET_CONCURRENCY_KEY
                ), List.of(resultSetType, resultSetConcurrency))
        );
    }

    private Map<String, Object> hashMapOf(List<String> keys, List<Object> values) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), values.get(i));
        }
        return map;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        String remoteCallableStatementUUID = this.callProxy(CallType.CALL_PREPARE, "Call", String.class,
                Arrays.asList(sql, resultSetType, resultSetConcurrency));
        return new org.openjdbcproxy.jdbc.CallableStatement(this, this.statementService, remoteCallableStatementUUID);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "TypeMap", Map.class);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.callProxy(CallType.CALL_SET, "TypeMap", Void.class, Arrays.asList(map));
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Holdability", Void.class, Arrays.asList(holdability));
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Holdability", Integer.class);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        String uuid = this.callProxy(CallType.CALL_SET, "Savepoint", String.class);
        return new org.openjdbcproxy.jdbc.Savepoint(uuid, this.statementService, this);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        String uuid = this.callProxy(CallType.CALL_SET, "Savepoint", String.class);
        return new org.openjdbcproxy.jdbc.Savepoint(uuid, this.statementService, this);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        this.callProxy(CallType.CALL_ROLLBACK, "", Void.class);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        org.openjdbcproxy.jdbc.Savepoint ojpSavepoint = (org.openjdbcproxy.jdbc.Savepoint) savepoint;
        this.callProxy(CallType.CALL_RELEASE, "Savepoint", Void.class,
                Arrays.asList(ojpSavepoint.getSavepointUUID()));
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new Statement(this, statementService, this.hashMapOf(
                List.of(CommonConstants.STATEMENT_RESULT_SET_TYPE_KEY,
                        CommonConstants.STATEMENT_RESULT_SET_CONCURRENCY_KEY,
                        CommonConstants.STATEMENT_RESULT_SET_HOLDABILITY_KEY)
                , List.of(resultSetType, resultSetConcurrency, resultSetHoldability)));
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                                       int resultSetHoldability) throws SQLException {
        return new PreparedStatement(this, sql, this.statementService, this.hashMapOf(
                List.of(CommonConstants.STATEMENT_RESULT_SET_TYPE_KEY,
                        CommonConstants.STATEMENT_RESULT_SET_CONCURRENCY_KEY,
                        CommonConstants.STATEMENT_RESULT_SET_HOLDABILITY_KEY)
                , List.of(resultSetType, resultSetConcurrency, resultSetHoldability)));
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        String remoteCallableStatementUUID = this.callProxy(CallType.CALL_PREPARE, "Call", String.class,
                Arrays.asList(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
        return new org.openjdbcproxy.jdbc.CallableStatement(this, this.statementService, remoteCallableStatementUUID);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new PreparedStatement(this, sql, this.statementService, this.hashMapOf(
                List.of(CommonConstants.STATEMENT_AUTO_GENERATED_KEYS_KEY)
                , List.of(autoGeneratedKeys)));
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new PreparedStatement(this, sql, this.statementService, this.hashMapOf(
                List.of(CommonConstants.STATEMENT_COLUMN_INDEXES_KEY)
                , List.of(columnIndexes)));
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        List values = new ArrayList();
        values.add(columnNames);
        return new PreparedStatement(this, sql, this.statementService, this.hashMapOf(
                List.of(CommonConstants.STATEMENT_COLUMN_NAMES_KEY)
                , values));
    }

    @Override
    public Clob createClob() throws SQLException {
        return new org.openjdbcproxy.jdbc.Clob(this, new LobServiceImpl(this, this.statementService),
                this.statementService,
                null
        );
    }

    @Override
    public Blob createBlob() throws SQLException {
        return new org.openjdbcproxy.jdbc.Blob(this, new LobServiceImpl(this, this.statementService),
                this.statementService,
                null
        );
    }

    @Override
    public NClob createNClob() throws SQLException {
        return new org.openjdbcproxy.jdbc.NClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return new org.openjdbcproxy.jdbc.SQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this.callProxy(CallType.CALL_IS, "Valid", Boolean.class, Arrays.asList(timeout));
    }

    @SneakyThrows //TODO revisit, maybe can be transferred from server and parsed in the client
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        this.callProxy(CallType.CALL_SET, "ClientInfo", Void.class, Arrays.asList(name, value));
    }

    @SneakyThrows //TODO revisit, maybe can be transferred from server and parsed in the client
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.callProxy(CallType.CALL_SET, "ClientInfo", Void.class, Arrays.asList(properties));
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "ClientInfo", String.class, Arrays.asList(name));
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "ClientInfo", Properties.class);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return new org.openjdbcproxy.jdbc.Array();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported.");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Schema", Void.class, Arrays.asList(schema));
    }

    @Override
    public String getSchema() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Schema", String.class);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported.");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NetworkTimeout", Void.class,
                Arrays.asList(executor, milliseconds));
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NetworkTimeout", Integer.class);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Cannot unwrap remote proxy object.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    private CallResourceRequest.Builder newCallBuilder() {
        return CallResourceRequest.newBuilder()
                .setSession(this.session)
                .setResourceType(ResourceType.RES_CONNECTION);
    }

    private <T> T callProxy(CallType callType, String targetName, Class returnType) throws SQLException {
        return this.callProxy(callType, targetName, returnType, Constants.EMPTY_OBJECT_LIST);
    }

    private <T> T callProxy(CallType callType, String targetName, Class returnType, List<Object> params) throws SQLException {
        CallResourceRequest.Builder reqBuilder = this.newCallBuilder();
        reqBuilder.setTarget(
                TargetCall.newBuilder()
                        .setCallType(callType)
                        .setResourceName(targetName)
                        .setParams(ByteString.copyFrom(serialize(params)))
                        .build()
        );
        CallResourceResponse response = this.statementService.callResource(reqBuilder.build());
        this.session = response.getSession();
        this.setSession(response.getSession());
        if (Void.class.equals(returnType)) {
            return null;
        }
        return (T) deserialize(response.getValues().toByteArray(), returnType);
    }
}
