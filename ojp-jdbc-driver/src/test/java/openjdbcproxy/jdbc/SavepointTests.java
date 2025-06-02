package openjdbcproxy.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Savepoint;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class SavepointTests {

    private Connection connection;

    @BeforeEach
    public void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "");
        connection.setAutoCommit(false);
        connection.createStatement().execute(
            "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(255))"
        );
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) connection.close();
    }

    @Test
    public void testSavepoint() throws Exception {
        connection.createStatement().execute("INSERT INTO test_table (id, name) VALUES (1, 'Alice')");
        Savepoint savepoint = connection.setSavepoint();

        connection.createStatement().execute("INSERT INTO test_table (id, name) VALUES (2, 'Bob')");
        connection.rollback(savepoint);

        ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM test_table");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt("id"));
    }
}