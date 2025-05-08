package openjdbcproxy.jdbc;

import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ResultSetMetaDataTests {

    private Connection connection;
    private ResultSetMetaData metaData;

    @SneakyThrows
    public void setUp(String driverClass, String url, String user, String password) throws SQLException {
        Class.forName(driverClass);
        connection = DriverManager.getConnection(url, user, password);
        Statement statement = connection.createStatement();

        try {
            statement.execute(
                    "DROP TABLE TEST_TABLE_METADATA"
            );
        }catch (Exception e) {
            //Might not be created.
        }

        statement.execute(
                "CREATE TABLE TEST_TABLE_METADATA (" +
                        "id INT AUTO_INCREMENT PRIMARY KEY, " +
                        "name VARCHAR(255) NOT NULL, " +
                        "age INT NULL, " +
                        "salary NUMERIC(10, 2) NOT NULL" +
                        ")"
        );
        statement.execute("INSERT INTO TEST_TABLE_METADATA (name, age, salary) VALUES ('Alice', 30, 50000.00)");

        ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST_TABLE_METADATA");
        metaData = resultSet.getMetaData();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) connection.close();
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testColumnProperties(String driverClass, String url, String user, String password) throws SQLException {
        setUp(driverClass, url, user, password);

        assertEquals(4, metaData.getColumnCount());
        // Column 1: id
        assertTrue(metaData.isAutoIncrement(1));
        assertTrue(metaData.isCaseSensitive(1));
        assertTrue(metaData.isSearchable(1));
        assertFalse(metaData.isCurrency(1));
        assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(1));
        assertTrue(metaData.isSigned(1));
        assertEquals(11, metaData.getColumnDisplaySize(1));
        assertEquals("ID", metaData.getColumnLabel(1));
        assertEquals("ID", metaData.getColumnName(1));
        assertEquals("PUBLIC", metaData.getSchemaName(1));
        assertEquals(32, metaData.getPrecision(1));
        assertEquals(0, metaData.getScale(1));
        assertEquals("TEST_TABLE_METADATA", metaData.getTableName(1));
        assertEquals("TEST", metaData.getCatalogName(1));
        assertEquals(Types.INTEGER, metaData.getColumnType(1));
        assertEquals("INTEGER", metaData.getColumnTypeName(1));
        assertFalse(metaData.isReadOnly(1));
        assertTrue(metaData.isWritable(1));
        assertFalse(metaData.isDefinitelyWritable(1));
        assertEquals("java.lang.Integer", metaData.getColumnClassName(1));

        // Column 2: name
        assertFalse(metaData.isAutoIncrement(2));
        assertTrue(metaData.isCaseSensitive(2));
        assertTrue(metaData.isSearchable(2));
        assertFalse(metaData.isCurrency(2));
        assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(2));
        assertFalse(metaData.isSigned(2));
        assertEquals(255, metaData.getColumnDisplaySize(2));
        assertEquals("NAME", metaData.getColumnLabel(2));
        assertEquals("NAME", metaData.getColumnName(2));
        assertEquals("PUBLIC", metaData.getSchemaName(2));
        assertEquals(255, metaData.getPrecision(2));
        assertEquals(0, metaData.getScale(2));
        assertEquals("TEST_TABLE_METADATA", metaData.getTableName(2));
        assertEquals("TEST", metaData.getCatalogName(2));
        assertEquals(Types.VARCHAR, metaData.getColumnType(2));
        assertEquals("CHARACTER VARYING", metaData.getColumnTypeName(2));
        assertFalse(metaData.isReadOnly(2));
        assertTrue(metaData.isWritable(2));
        assertFalse(metaData.isDefinitelyWritable(2));
        assertEquals("java.lang.String", metaData.getColumnClassName(2));

        // Column 3: age
        assertFalse(metaData.isAutoIncrement(3));
        assertTrue(metaData.isCaseSensitive(3));
        assertTrue(metaData.isSearchable(3));
        assertFalse(metaData.isCurrency(3));
        assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(3));
        assertTrue(metaData.isSigned(3));
        assertEquals(11, metaData.getColumnDisplaySize(3));
        assertEquals("AGE", metaData.getColumnLabel(3));
        assertEquals("AGE", metaData.getColumnName(3));
        assertEquals("PUBLIC", metaData.getSchemaName(3));
        assertEquals(32, metaData.getPrecision(3));
        assertEquals(0, metaData.getScale(3));
        assertEquals("TEST_TABLE_METADATA", metaData.getTableName(3));
        assertEquals("TEST", metaData.getCatalogName(3));
        assertEquals(Types.INTEGER, metaData.getColumnType(3));
        assertEquals("INTEGER", metaData.getColumnTypeName(3));
        assertFalse(metaData.isReadOnly(3));
        assertTrue(metaData.isWritable(3));
        assertFalse(metaData.isDefinitelyWritable(3));
        assertEquals("java.lang.Integer", metaData.getColumnClassName(3));

        // Column 4: salary
        assertFalse(metaData.isAutoIncrement(4));
        assertTrue(metaData.isCaseSensitive(4));
        assertTrue(metaData.isSearchable(4));
        assertFalse(metaData.isCurrency(4));
        assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(4));
        assertTrue(metaData.isSigned(4));
        assertEquals(12, metaData.getColumnDisplaySize(4));
        assertEquals("SALARY", metaData.getColumnLabel(4));
        assertEquals("SALARY", metaData.getColumnName(4));
        assertEquals("PUBLIC", metaData.getSchemaName(4));
        assertEquals(10, metaData.getPrecision(4));
        assertEquals(2, metaData.getScale(4));
        assertEquals("TEST_TABLE_METADATA", metaData.getTableName(4));
        assertEquals("TEST", metaData.getCatalogName(4));
        assertEquals(Types.NUMERIC, metaData.getColumnType(4));
        assertEquals("NUMERIC", metaData.getColumnTypeName(4));
        assertFalse(metaData.isReadOnly(4));
        assertTrue(metaData.isWritable(4));
        assertFalse(metaData.isDefinitelyWritable(4));
        assertEquals("java.math.BigDecimal", metaData.getColumnClassName(4));
    }
}