package openjdbcproxy.jdbc;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MultipleTypesIntegrationTest {

    @Test
    public void typesCoverageTestSuccessful() throws SQLException, ClassNotFoundException, ParseException {
        Class.forName("org.openjdbcproxy.jdbc.Driver");
        Connection conn = DriverManager.
                getConnection("jdbc:ojp_h2:~/test", "sa", "");

        try {
            this.executeUpdate(conn,
                    """
                            drop table test_table
                            """);
        } catch (Exception e) {
            //Might not find it, not an issue
        }
        this.executeUpdate(conn,
                """
                create table test_table(
                         val_int              INT NOT NULL,
                         val_varchar          VARCHAR(50) NOT NULL,
                         val_double_precision DOUBLE PRECISION,
                         val_bigint           BIGINT,
                         val_tinyint          TINYINT,
                         val_smallint         SMALLINT,
                         val_boolean          BOOLEAN,
                         val_decimal          DECIMAL,
                         val_float            FLOAT(2),
                         val_byte             BINARY,
                         val_binary           BINARY(4),
                         val_date             DATE)
                """);

        java.sql.PreparedStatement psInsert = conn.prepareStatement(
                """
                    insert into test_table (val_int, val_varchar, val_double_precision, val_bigint, val_tinyint,
                    val_smallint, val_boolean, val_decimal, val_float, val_byte, val_binary, val_date)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
        );

        psInsert.setInt(1, 1);
        psInsert.setString(2, "TITLE_1");
        psInsert.setDouble(3, 2.2222d);
        psInsert.setLong(4, 33333333333333l);
        psInsert.setInt(5, 127);
        psInsert.setInt(6, 32767);
        psInsert.setBoolean(7, true);
        psInsert.setBigDecimal(8, new BigDecimal(10));
        psInsert.setFloat(9, 20.20f);
        psInsert.setByte(10, (byte) 1);
        psInsert.setBytes(11, "AAAA".getBytes());
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        psInsert.setDate(12, new Date(sdf.parse("29/03/2025").getTime()));
        psInsert.executeUpdate();

        java.sql.PreparedStatement psSelect = conn.prepareStatement("select * from test_table where val_int = ?");
        psSelect.setInt(1, 1);
        ResultSet resultSet = psSelect.executeQuery();
        resultSet.next();
        Assert.assertEquals(1, resultSet.getInt(1));
        Assert.assertEquals("TITLE_1", resultSet.getString(2));
        Assert.assertEquals("2.2222", ""+resultSet.getDouble(3));
        Assert.assertEquals(33333333333333L, resultSet.getLong(4));
        Assert.assertEquals(127, resultSet.getInt(5));
        Assert.assertEquals(32767, resultSet.getInt(6));
        Assert.assertEquals(true, resultSet.getBoolean(7));
        Assert.assertEquals(new BigDecimal(10), resultSet.getBigDecimal(8));
        Assert.assertEquals(20.20f+"", ""+resultSet.getFloat(9));
        Assert.assertEquals((byte) 1, resultSet.getByte(10));
        Assert.assertEquals("AAAA", new String(resultSet.getBytes(11)));
        Assert.assertEquals("29/03/2025", sdf.format(resultSet.getDate(12)));

        Assert.assertEquals(1, resultSet.getInt("val_int"));
        Assert.assertEquals("TITLE_1", resultSet.getString("val_varchar"));
        Assert.assertEquals("2.2222", ""+resultSet.getDouble("val_double_precision"));
        Assert.assertEquals(33333333333333L, resultSet.getLong("val_bigint"));
        Assert.assertEquals(127, resultSet.getInt("val_tinyint"));
        Assert.assertEquals(32767, resultSet.getInt("val_smallint"));
        Assert.assertEquals(new BigDecimal(10), resultSet.getBigDecimal("val_decimal"));
        Assert.assertEquals(20.20f+"", ""+resultSet.getFloat("val_float"));
        Assert.assertEquals(true, resultSet.getBoolean("val_boolean"));
        Assert.assertEquals((byte) 1, resultSet.getByte("val_byte"));
        Assert.assertEquals("AAAA", new String(resultSet.getBytes("val_binary")));
        Assert.assertEquals("29/03/2025", sdf.format(resultSet.getDate("val_date")));

        executeUpdate(conn,
                """
                    delete from test_table where val_int=1
                    """
        );

        ResultSet resultSetAfterDeletion = psSelect.executeQuery();
        Assert.assertFalse(resultSetAfterDeletion.next());

        resultSet.close();
        psSelect.close();
        conn.close();
    }

    private int executeUpdate(Connection conn, String s) throws SQLException {
        try (Statement stmt =  conn.createStatement()) {
            return stmt.executeUpdate(s);
        }
    }

}
