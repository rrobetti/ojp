package openjdbcproxy.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class DatabaseMetaDataBasicTests {

    private Connection connection;

    public void setUp(String driverClass, String url, String user, String password) throws Exception {
        Class.forName(driverClass);
        connection = DriverManager.getConnection(url, user, password);
        try {
            connection.createStatement().execute(
                    "DROP TABLE test_table"
            );
        } catch (SQLException e) {
            //Do nothing.
        }

        connection.createStatement().execute(
            "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(255))"
        );
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) connection.close();
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testDatabaseMetadata(String driverClass, String url, String user, String password) throws Exception {
        this.setUp(driverClass, url, user, password);
        DatabaseMetaData metaData = connection.getMetaData();

        assertNotNull(metaData);
        assertEquals("H2", metaData.getDatabaseProductName());

        ResultSet tables = metaData.getTables(null, null, "%TEST%", null);
        assertTrue(tables.next());
        assertEquals("TEST_TABLE", tables.getString("TABLE_NAME"));
    }
}