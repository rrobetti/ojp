package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.TransactionStatus;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.openjdbcproxy.grpc.client.StatementService;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class Connection implements java.sql.Connection {

    @Getter
    @Setter
    private SessionInfo session;
    private final StatementService statementService;
    private boolean autoCommit = true;
    private boolean readOnly = false;
    private SessionTerminationTrigger sessionTerminationTrigger;

    public Connection(SessionInfo session, StatementService statementService) {
        this.session = session;
        this.statementService = statementService;
        this.sessionTerminationTrigger = new SessionTerminationTrigger();
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
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return "";
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

        if (!this.autoCommit && autoCommit) {
            if (this.sessionTerminationTrigger.triggerIssued(true, null, null)) {
                this.close();
            }
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
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;

        if (readOnly) {
            if (this.sessionTerminationTrigger.triggerIssued(null, true, null)) {
                this.close();
            }
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return "";
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (this.sessionTerminationTrigger.triggerIssued(null, null, true)) {
            this.close();
        }
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return Map.of();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
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
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return "";
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return "";
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
