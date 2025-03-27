package openjdbcproxy.jdbc;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MultipleTypesIntegrationTest {

    @Test
    public void typesCoverageTestSuccessful() throws SQLException, ClassNotFoundException {
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
                         val_binary           BINARY)
                """);

        /*java.sql.PreparedStatement psInsert = conn.prepareStatement(
                """
                    insert into test_table (val_int, val_varchar, val_double_precision, val_bigint, val_tinyint,
                    val_smallint, val_boolean, val_decimal, val_float, val_binary)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
        );

        psInsert.setInt(1, 1);
        psInsert.setString(2, "TITLE_1");*/


        this.executeUpdate(conn,
                """
                    insert into test_table (val_int, val_varchar, val_double_precision, val_bigint, val_tinyint,
                    val_smallint, val_boolean, val_decimal, val_float, val_binary)
                    values (1, 'TITLE_1', 2.2222, 33333333333333, 127, 32767, 1, 10, 20.20, X'10')
                    """
        );

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
        Assert.assertEquals(""+resultSet.getBytes("val_binary"), ""+resultSet.getBytes(10));

        Assert.assertEquals(1, resultSet.getInt("val_int"));
        Assert.assertEquals("TITLE_1", resultSet.getString("val_varchar"));
        Assert.assertEquals("2.2222", ""+resultSet.getDouble("val_double_precision"));
        Assert.assertEquals(33333333333333L, resultSet.getLong("val_bigint"));
        Assert.assertEquals(127, resultSet.getInt("val_tinyint"));
        Assert.assertEquals(32767, resultSet.getInt("val_smallint"));
        Assert.assertEquals(new BigDecimal(10), resultSet.getBigDecimal("val_decimal"));
        Assert.assertEquals(20.20f+"", ""+resultSet.getFloat("val_float"));
        Assert.assertEquals(true, resultSet.getBoolean("val_boolean"));

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
