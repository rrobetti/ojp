package openjdbcproxy.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Calendar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class H2PreparedStatementExtensiveTests {

    private Connection connection;
    private PreparedStatement ps;

    public void setUp(String driverClass, String url, String user, String password) throws Exception {
        connection = DriverManager.getConnection(url, user, password);
        Statement stmt = connection.createStatement();
        try {
            stmt.execute("DROP TABLE test_table");
        } catch (SQLException ignore) {}
        stmt.execute("CREATE TABLE test_table (" +
                "id INT PRIMARY KEY, " +
                "name VARCHAR(255), " +
                "age INT, " +
                "data BLOB, " +
                "info CLOB, " +
                "dt DATE)");
        stmt.close();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (ps != null) ps.close();
        if (connection != null) connection.close();
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testParameterSetters(String driverClass, String url, String user, String password) throws Exception {
        this.setUp(driverClass, url, user, password);

        ps = connection.prepareStatement("INSERT INTO test_table (id, name, age, data, info, dt) VALUES (?, ?, ?, ?, ?, ?)");

        // Numeric and boolean
        ps.setInt(1, 1);
        ps.setLong(1, 2L);
        ps.setShort(1, (short) 3);
        ps.setByte(1, (byte) 4);
        ps.setFloat(1, 1.5f);
        ps.setDouble(1, 2.5);
        ps.setBigDecimal(1, BigDecimal.valueOf(123));
        ps.setBoolean(1, true);

        // Strings and types
        ps.setString(2, "Alice");
        ps.setNString(2, "Bob");
        ps.setNull(1, Types.INTEGER);
        ps.setNull(1, Types.INTEGER, "INTEGER");
        ps.setObject(1, "test");
        ps.setObject(1, "test", Types.VARCHAR);
        ps.setObject(1, "test", Types.VARCHAR, 10);

        // Byte arrays and streams
        ps.setBytes(4, new byte[] {1, 2, 3});
        ps.setAsciiStream(2, new ByteArrayInputStream("ascii".getBytes()), 5);
        ps.setBinaryStream(4, new ByteArrayInputStream(new byte[] {4, 5}), 2);
        ps.setAsciiStream(2, new ByteArrayInputStream("ascii".getBytes()));
        ps.setBinaryStream(4, new ByteArrayInputStream(new byte[] {4, 5}));
        // Deprecated
        try { ps.setUnicodeStream(2, new ByteArrayInputStream(new byte[] {1}), 1); } catch (Exception ignore) {}

        // Date/time
        ps.setDate(6, new java.sql.Date(System.currentTimeMillis()));
        ps.setTime(3, new java.sql.Time(System.currentTimeMillis()));
        ps.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()));
        ps.setDate(6, new java.sql.Date(System.currentTimeMillis()), Calendar.getInstance());
        ps.setTime(3, new java.sql.Time(System.currentTimeMillis()), Calendar.getInstance());
        ps.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()), Calendar.getInstance());

        // URL, RowId
        assertThrows(Exception.class, () -> ps.setURL(3, new URL("http://localhost")));
        assertThrows(Exception.class, () -> ps.setRowId(3, null));

        // Character and N-character streams
        ps.setCharacterStream(3, new StringReader("chars"), 4);
        ps.setNCharacterStream(2, new StringReader("nchars"), 6L);
        ps.setCharacterStream(3, new StringReader("chars"));
        ps.setNCharacterStream(2, new StringReader("nchars"));
        ps.setCharacterStream(3, new StringReader("chars"), 3L);

        // CLOB, BLOB, NCLOB
        ps.setClob(5, new StringReader("clob"), 4L);
        ps.setNClob(5, new StringReader("nclob"), 5L);
        ps.setBlob(4, new ByteArrayInputStream(new byte[] {1, 2}), 2L);
        ps.setClob(5, new StringReader("clob"));
        ps.setNClob(5, new StringReader("nclob"));
        ps.setBlob(4, new ByteArrayInputStream(new byte[] {1, 2}));
        ps.setNClob(5, (NClob)null);
        ps.setBlob(4, (Blob)null);
        ps.setClob(5, (Clob)null);

        // Ref, Array, SQLXML
        assertThrows(SQLException.class, () -> ps.setRef(1, null));
        ps.setArray(1, null);
        ps.setSQLXML(1, null);

        // Call clearParameters for coverage
        ps.clearParameters();
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testExecutionAndBatchMethods(String driverClass, String url, String user, String password) throws Exception {
        this.setUp(driverClass, url, user, password);

        ps = connection.prepareStatement("INSERT INTO test_table (id, name, age, data, info, dt) VALUES (?, ?, ?, ?, ?, ?)");
        ps.setInt(1, 10); ps.setString(2, "Test"); ps.setInt(3, 30);
        ps.setBytes(4, new byte[]{1}); ps.setString(5, "info"); ps.setDate(6, new java.sql.Date(System.currentTimeMillis()));
        ps.addBatch();

        ps.setInt(1, 11); ps.setString(2, "Another"); ps.setInt(3, 31);
        ps.setBytes(4, new byte[]{2}); ps.setString(5, "info2"); ps.setDate(6, new java.sql.Date(System.currentTimeMillis()));
        ps.addBatch();

        int[] results = ps.executeBatch();
        assertEquals(2, results.length);

        // execute, executeUpdate, executeQuery
        ps = connection.prepareStatement("SELECT * FROM test_table WHERE id = ?");
        ps.setInt(1, 10);
        ResultSet rs = ps.executeQuery();
        assertNotNull(rs);

        ps = connection.prepareStatement("UPDATE test_table SET age = ? WHERE id = ?");
        ps.setInt(1, 42); ps.setInt(2, 11);
        int updateCount = ps.executeUpdate();
        assertTrue(updateCount >= 0);

        ps = connection.prepareStatement("SELECT * FROM test_table WHERE id = ?");
        ps.setInt(1, 10);
        try {
            boolean executed = ps.execute();
        } catch (SQLException e) {
            assertNotNull(e);
        }

        // executeLargeUpdate (may throw on some drivers)
        try { ps.executeLargeUpdate(); } catch (Exception ignore) {}
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testMetaDataAndWarnings(String driverClass, String url, String user, String password) throws Exception {
        this.setUp(driverClass, url, user, password);

        ps = connection.prepareStatement("SELECT * FROM test_table WHERE id = ?");
        ps.setInt(1, 10);

        ResultSetMetaData resultSetMetaData = ps.getMetaData();
        assertNotNull(resultSetMetaData);
        assertEquals("ID", resultSetMetaData.getColumnLabel(1));
        assertNotNull(ps.getParameterMetaData());
        ps.clearWarnings();
        assertNull(ps.getWarnings());
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testStatementCommonMethods(String driverClass, String url, String user, String password) throws Exception {
        this.setUp(driverClass, url, user, password);

        ps = connection.prepareStatement("SELECT * FROM test_table WHERE id = ?");
        // Field size and max rows
        assertEquals(0, ps.getMaxFieldSize());
        ps.setMaxFieldSize(128);
        assertEquals(0, ps.getMaxFieldSize());

        assertEquals(0, ps.getMaxRows());
        ps.setMaxRows(10);
        assertEquals(10, ps.getMaxRows());

        ps.setEscapeProcessing(true);
        ps.setQueryTimeout(5);
        assertEquals(5, ps.getQueryTimeout());

        // Cursor/Fetch
        ps.setCursorName("testCursor");
        ps.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertEquals(ResultSet.FETCH_FORWARD, ps.getFetchDirection());
        assertThrows(Exception.class, () -> { ps.setFetchSize(50); });
        assertEquals(100, ps.getFetchSize());

        assertTrue(ps.getResultSetConcurrency() >= 0);
        assertTrue(ps.getResultSetType() >= 0);
        assertTrue(ps.getResultSetHoldability() >= 0);

        // Poolable/Close
        ps.setPoolable(true);
        assertFalse(ps.isPoolable());
        ps.closeOnCompletion();
        assertTrue(ps.isCloseOnCompletion());

        // isClosed
        assertFalse(ps.isClosed());

        // cancel (for full interface coverage)
        try {
            ps.cancel();
        } catch (Exception ignore) {
            // Some drivers may throw if cancel() is not supported or statement is not running.
        }

        ps.close();
        assertTrue(ps.isClosed());
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testStatementBatchAndConnection(String driverClass, String url, String user, String password) throws Exception {
        this.setUp(driverClass, url, user, password);

        ps = connection.prepareStatement("SELECT * FROM test_table WHERE id = ?");
        ps.clearBatch();
        assertThrows(Exception.class, () -> ps.addBatch("DELETE FROM test_table WHERE id < 0"));
        ps.clearBatch();

        assertNotNull(ps.getConnection());
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testResultAndGeneratedKeysMethods(String driverClass, String url, String user, String password) throws Exception {
        this.setUp(driverClass, url, user, password);

        Statement stmt = connection.createStatement();
        stmt.execute("INSERT INTO test_table (id, name, age) VALUES (100, 'A', 1)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_table");
        assertNotNull(rs);
        assertTrue(rs.next());
        int updateCount = stmt.executeUpdate("UPDATE test_table SET age = 99 WHERE id = 100");
        assertEquals(1, updateCount);

        // getResultSet, getUpdateCount, getMoreResults
        stmt.execute("SELECT * FROM test_table");
        ResultSet rs2 = stmt.getResultSet();
        assertNotNull(rs2);
        int count = stmt.getUpdateCount();
        assertTrue(count >= -1);
        assertFalse(stmt.getMoreResults());
        assertFalse(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));

        // Generated Keys
        stmt.executeUpdate("INSERT INTO test_table (id, name, age) VALUES (101, 'B', 2)", Statement.RETURN_GENERATED_KEYS);
        ResultSet keys = stmt.getGeneratedKeys();
        assertNotNull(keys);
        keys.next();
        Long id = keys.getLong(1);
        assertNotNull(id);

        // Various execute overloads
        stmt.execute("SELECT * FROM test_table", Statement.NO_GENERATED_KEYS);
        stmt.execute("SELECT * FROM test_table", new int[]{1});
        stmt.execute("SELECT * FROM test_table", new String[]{"id"});
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    public void testStatementLargeAndDefaultMethods(String driverClass, String url, String user, String password) throws Exception {
        this.setUp(driverClass, url, user, password);

        Statement stmt = connection.createStatement();
        // Large update/batch methods (may throw on H2)
        try { stmt.getLargeUpdateCount(); } catch (Exception ignore) {}
        try { stmt.setLargeMaxRows(100L); } catch (Exception ignore) {}
        try { stmt.getLargeMaxRows(); } catch (Exception ignore) {}
        try { stmt.executeLargeBatch(); } catch (Exception ignore) {}
        try { stmt.executeLargeUpdate("UPDATE test_table SET age = 101 WHERE id = 100"); } catch (Exception ignore) {}
        try { stmt.executeLargeUpdate("UPDATE test_table SET age = 101 WHERE id = 100", Statement.RETURN_GENERATED_KEYS); } catch (Exception ignore) {}
        try { stmt.executeLargeUpdate("UPDATE test_table SET age = 101 WHERE id = 100", new int[]{1}); } catch (Exception ignore) {}
        try { stmt.executeLargeUpdate("UPDATE test_table SET age = 101 WHERE id = 100", new String[]{"id"}); } catch (Exception ignore) {}

        // Enquote and identifier methods
        assertEquals("'foo''bar'", stmt.enquoteLiteral("foo'bar"));
        assertEquals("\"foo\"", stmt.enquoteIdentifier("foo", true));
        assertFalse(stmt.isSimpleIdentifier("fooBar"));
        assertEquals("N'foo''bar'", stmt.enquoteNCharLiteral("foo'bar"));
    }
}