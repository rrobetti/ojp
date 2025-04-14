package org.openjdbcproxy.grpc.server;

import com.openjdbcproxy.grpc.LobType;
import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.TransactionStatus;
import lombok.extern.slf4j.Slf4j;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SessionManagerImpl implements SessionManager{

    private Map<String, String> connectionHashMap = new ConcurrentHashMap<>();
    private Map<String, Session> sessionMap = new ConcurrentHashMap<>();

    @Override
    public void registerClientUUID(String connectionHash, String clientUUID) {
        log.info("Registering client uuid {}", clientUUID);
        this.connectionHashMap.put(clientUUID, connectionHash);
    }

    @Override
    public SessionInfo createSession(String clientUUID, Connection connection) {
        System.out.println("Create session for client uuid " + clientUUID);
        Session session = new Session(connection, connectionHashMap.get(clientUUID), clientUUID);
        System.out.println("Session " + session.getSessionUUID() + " created for client uuid " + clientUUID);
        this.sessionMap.put(session.getSessionUUID(), session);
        return session.getSessionInfo();
    }

    @Override
    public Connection getConnection(SessionInfo sessionInfo) {
        log.debug("Getting a connection for session {}", sessionInfo.getSessionUUID() );
        return this.sessionMap.get(sessionInfo.getSessionUUID()).getConnection();
    }

    @Override
    public String registerResultSet(SessionInfo sessionInfo, ResultSet rs) {
        String uuid = UUID.randomUUID().toString();
        this.sessionMap.get(sessionInfo.getSessionUUID()).addResultSet(uuid, rs);
        return uuid;
    }

    @Override
    public ResultSet getResultSet(SessionInfo sessionInfo, String uuid) {
        return this.sessionMap.get(sessionInfo.getSessionUUID()).getResultSet(uuid);
    }

    @Override
    public String registerStatement(SessionInfo sessionInfo, Statement stmt) {
        String uuid = UUID.randomUUID().toString();
        this.sessionMap.get(sessionInfo.getSessionUUID()).addStatement(uuid, stmt);
        return uuid;
    }

    @Override
    public Statement getStatement(SessionInfo sessionInfo, String uuid) {
        return this.sessionMap.get(sessionInfo.getSessionUUID()).getStatement(uuid);
    }

    @Override
    public String registerPreparedStatement(SessionInfo sessionInfo, PreparedStatement ps) {
        String uuid = UUID.randomUUID().toString();
        this.sessionMap.get(sessionInfo.getSessionUUID()).addPreparedStatement(uuid, ps);
        return uuid;
    }

    @Override
    public PreparedStatement getPreparedStatement(SessionInfo sessionInfo, String uuid) {
        return this.sessionMap.get(sessionInfo.getSessionUUID()).getPreparedStatement(uuid);
    }

    @Override
    public String registerLob(SessionInfo sessionInfo, Object blob) {
        String uuid = UUID.randomUUID().toString();
        this.sessionMap.get(sessionInfo.getSessionUUID()).addLob(uuid, blob);
        return uuid;
    }

    @Override
    public <T> T getLob(SessionInfo sessionInfo, String uuid) {
        return (T) this.sessionMap.get(sessionInfo.getSessionUUID()).getLob(uuid);
    }

    @Override
    public void terminateSession(SessionInfo sessionInfo) throws SQLException {
        System.out.println("Terminating session -> " + sessionInfo.getSessionUUID() );
        Session targetSession = this.sessionMap.remove(sessionInfo.getSessionUUID());

        if (TransactionStatus.TRX_ACTIVE.equals(sessionInfo.getTransactionInfo().getTransactionStatus())) {
            System.out.println("Rolling back active transaction");
            targetSession.getConnection().rollback();
        }
        targetSession.terminate();
    }
}
