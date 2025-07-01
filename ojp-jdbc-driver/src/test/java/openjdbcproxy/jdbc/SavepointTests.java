package openjdbcproxy.jdbc;

import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class SavepointTests {

    private Connection connection;

    @SneakyThrows
    public void setUp(String driverClass, String url, String user, String pwd) throws SQLException {
        connection = DriverManager.getConnection(url, user, pwd);
        connection.setAutoCommit(false);
        connection.createStatement().execute(
                "DROP TABLE IF EXISTS test_table"
        );
        connection.createStatement().execute(
            "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(255))"
        );
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) connection.close();
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/postgres_connection.csv")
    public void testSavepoint(String driverClass, String url, String user, String pwd) throws SQLException {
        setUp(driverClass, url, user, pwd);
        connection.createStatement().execute("INSERT INTO test_table (id, name) VALUES (1, 'Alice')");
        Savepoint savepoint = connection.setSavepoint();

        connection.createStatement().execute("INSERT INTO test_table (id, name) VALUES (2, 'Bob')");
        connection.rollback(savepoint);

        ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM test_table");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt("id"));
    }
}