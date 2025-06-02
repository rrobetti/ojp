package org.openjdbcproxy.grpc.server;

import com.openjdbcproxy.grpc.SessionInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds information about a session of a given client.
 */
@Slf4j
public class Session {
    @Getter
    private final String sessionUUID;
    @Getter
    private final String connectionHash;
    @Getter
    private final String clientUUID;
    @Getter
    private Connection connection;
    private Map<String, ResultSet> resultSetMap;
    private Map<String, Statement> statementMap;
    private Map<String, PreparedStatement> preparedStatementMap;
    private Map<String, CallableStatement> callableStatementMap;
    private Map<String, Object> lobMap;
    private Map<String, Object> attrMap;
    private boolean closed;

    public Session(Connection connection, String connectionHash, String clientUUID) {
        this.connection = connection;
        this.connectionHash = connectionHash;
        this.clientUUID = clientUUID;
        this.sessionUUID = UUID.randomUUID().toString();
        this.closed = false;
        this.resultSetMap = new ConcurrentHashMap<>();
        this.statementMap = new ConcurrentHashMap<>();
        this.preparedStatementMap = new ConcurrentHashMap<>();
        this.callableStatementMap = new ConcurrentHashMap<>();
        this.lobMap = new ConcurrentHashMap<>();
        this.attrMap = new ConcurrentHashMap<>();
    }

    public SessionInfo getSessionInfo() {
        log.info("get session info -> " + this.connectionHash);
        return SessionInfo.newBuilder()
                .setConnHash(this.connectionHash)
                .setClientUUID(this.clientUUID)
                .setSessionUUID(this.sessionUUID)
                .build();
    }

    public void addAttr(String key, Object value) {
        this.notClosed();
        this.attrMap.put(key, value);
    }

    public Object getAttr(String key) {
        this.notClosed();
        return this.attrMap.get(key);
    }

    public void addResultSet(String uuid, ResultSet rs) {
        this.notClosed();
        this.resultSetMap.put(uuid, rs);
    }

    public ResultSet getResultSet(String uuid) {
        this.notClosed();
        return this.resultSetMap.get(uuid);
    }

    public void addStatement(String uuid, Statement stmt) {
        this.notClosed();
        this.statementMap.put(uuid, stmt);
    }

    public Statement getStatement(String uuid) {
        this.notClosed();
        return this.statementMap.get(uuid);
    }

    public void addPreparedStatement(String uuid, PreparedStatement ps) {
        this.notClosed();
        this.preparedStatementMap.put(uuid, ps);
    }

    public PreparedStatement getPreparedStatement(String uuid) {
        this.notClosed();
        return this.preparedStatementMap.get(uuid);
    }

    public void addCallableStatement(String uuid, CallableStatement cs) {
        this.notClosed();
        this.callableStatementMap.put(uuid, cs);
    }

    public CallableStatement getCallableStatement(String uuid) {
        this.notClosed();
        return this.callableStatementMap.get(uuid);
    }

    public void addLob(String uuid, Object o) {
        this.notClosed();
        if (o != null) {
            this.lobMap.put(uuid, o);
        }
    }

    public <T> T getLob(String uuid) {
        this.notClosed();
        return (T) this.lobMap.get(uuid);
    }

    private void notClosed() {
        if (this.closed) {
            throw new RuntimeException("Session is closed.");
        }
    }

    public void terminate() throws SQLException {

        if (this.closed) {
            return;
        }

        //Closing the connection here means that the connection pool will close all resources associated with it and
        // reset the connection state before returning it to the pool.
        this.connection.close();

        //Clear session internal objects to free memory
        this.closed = true;
        this.lobMap = null;
        this.resultSetMap = null;
        this.statementMap = null;
        this.preparedStatementMap = null;
        this.connection = null;
        this.attrMap = null;
    }

    public Collection<Object> getAllLobs() {
        return this.lobMap.values();
    }
}
