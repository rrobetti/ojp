package org.openjdbcproxy.grpc.server;

import com.openjdbcproxy.grpc.SessionInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    private Map<String, Object> lobMap;
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
        this.lobMap = new ConcurrentHashMap<>();
    }

    public SessionInfo getSessionInfo() {
        System.out.println("get session info -> " + this.connectionHash);
        return SessionInfo.newBuilder()
                .setConnHash(this.connectionHash)
                .setClientUUID(this.clientUUID)
                .setSessionUUID(this.sessionUUID)
                .build();
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

    public void addLob(String uuid, Object o) {
        this.notClosed();
        this.lobMap.put(uuid, o);
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

        //Free all blobs
        for (Object o : this.lobMap.values()) {
            try {
                if (o instanceof Blob b) {
                    b.free();
                } else if (o instanceof Clob c) {
                    c.free();
                }
            } catch (Exception e) {
                log.error("Failed to free Blob: " + e.getMessage(), e);
            }
        }
        //Close all ResultSets
        for (ResultSet rs : this.resultSetMap.values()) {
            try {
                rs.close();
            } catch (Exception e) {
                log.error("Failed to close ResultSet: " + e.getMessage(), e);
            }
        }
        //Close all Statements
        for (Statement stmt : this.statementMap.values()) {
            try {
                stmt.close();
            } catch (Exception e) {
                log.error("Failed to close Statement: " + e.getMessage(), e);
            }
        }
        //Close all PreparedStatements
        for (PreparedStatement ps : this.preparedStatementMap.values()) {
            try {
                ps.close();
            } catch (Exception e) {
                log.error("Failed to close PreparedStatement: " + e.getMessage(), e);
            }
        }

        //If an exception happens closing the actual connection, propagate it to the client.
        this.connection.close();

        //Clear internal objects
        this.closed = true;
        this.lobMap = null;
        this.resultSetMap = null;
        this.statementMap = null;
        this.preparedStatementMap = null;
        this.connection = null;
    }
}
