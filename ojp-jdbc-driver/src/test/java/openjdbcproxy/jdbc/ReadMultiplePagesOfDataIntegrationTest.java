package openjdbcproxy.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ReadMultiplePagesOfDataIntegrationTest {

    @Test
    public void multiplePagesOfRowsResultSetSuccessful() throws SQLException, ClassNotFoundException {
        Class.forName("org.openjdbcproxy.jdbc.Driver");
        Connection conn = DriverManager.
                getConnection("jdbc:ojp_h2:~/test", "sa", "");

        try {
            this.executeUpdate(conn,
                    """
                            drop table test_table_multi
                            """);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.executeUpdate(conn,
                """
                create table test_table_multi(
                         id INT NOT NULL,
                           title VARCHAR(50) NOT NULL)
                """);

        for (int i = 0; i < 150; i++) { //TODO make this test parameterized with multiple parameters
            this.executeUpdate(conn,
                    "insert into test_table_multi (id, title) values (" + i + ", 'TITLE_" + i + "')"
            );
        }

        java.sql.PreparedStatement psSelect = conn.prepareStatement("select * from test_table_multi");
        ResultSet resultSet = psSelect.executeQuery();

        for (int i = 0; i < 150; i++) {
            resultSet.next();
            int id = resultSet.getInt(1);
            String title = resultSet.getString(2);
            Assert.assertEquals(i, id);
            Assert.assertEquals("TITLE_" + i, title);
        }

        executeUpdate(conn,
                """
                    delete from test_table_multi
                    """
        );

        ResultSet resultSetAfterDeletion = psSelect.executeQuery();
        Assert.assertFalse(resultSetAfterDeletion.next());

        conn.close();
    }

    private int executeUpdate(Connection conn, String s) throws SQLException {
        try (Statement stmt =  conn.createStatement()) {
            return stmt.executeUpdate(s);
        }
    }

}
