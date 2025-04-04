package org.openjdbcproxy.grpc.server;

import com.openjdbcproxy.grpc.SessionInfo;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Manages sessions established with clients
 */
public interface SessionManager {
    void registerClientUUID(String connectionHash, String clientUUID);
    SessionInfo createSession(String clientUUID, Connection connection);
    Connection getConnection(SessionInfo sessionInfo);
    String registerResultSet(SessionInfo sessionInfo, ResultSet rs);
    ResultSet getResultSet(SessionInfo sessionInfo, String uuid);
    String registerStatement(SessionInfo sessionInfo, Statement stmt);
    Statement getStatement(SessionInfo sessionInfo, String uuid);
    String registerPreparedStatement(SessionInfo sessionInfo, PreparedStatement ps);
    PreparedStatement getPreparedStatement(SessionInfo sessionInfo, String uuid);
    String registerBlob(SessionInfo sessionInfo, Blob blob);
    Blob getBlob(SessionInfo sessionInfo, String uuid);
    void terminateSession(SessionInfo sessionInfo) throws SQLException;
}
