package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.SessionInfo;
import org.openjdbcproxy.grpc.client.StatementService;
import org.openjdbcproxy.grpc.client.StatementServiceGrpcClient;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static org.openjdbcproxy.jdbc.Constants.PASSWORD;
import static org.openjdbcproxy.jdbc.Constants.USER;

public class Driver implements java.sql.Driver {

    static {
        try {
            DriverManager.registerDriver(new Driver(new StatementServiceGrpcClient()));
        } catch (SQLException var1) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    private final StatementService statementService;

    public Driver(StatementService statementService) {
        this.statementService = statementService;
    }

    @Override
    public java.sql.Connection connect(String url, Properties info) throws SQLException {
        if (url.toUpperCase().contains("H2:")) {
            DbInfo.setH2DB(true);
        } else {
            DbInfo.setH2DB(false);
        }
        SessionInfo sessionInfo = this.statementService
                .connect(ConnectionDetails.newBuilder()
                        .setUrl(url)
                        .setUser((String) info.get(USER))
                        .setPassword((String) info.get(PASSWORD))
                        .setClientUUID(ClientUUID.getUUID())
                        .build()
                );
        //TODO create centralized handling of exceptions returned that coverts automatically to SQLException.
        return new Connection(sessionInfo, this.statementService);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("URL is null");
        } else if (url.startsWith("jdbc:ojp_")) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
