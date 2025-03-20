package openjdbcproxy.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BasicCrudIntegrationTest {

    @Test
    public void crudTestSuccessful() throws SQLException, ClassNotFoundException {
        Class.forName("org.openjdbcproxy.jdbc.Driver");
        Connection conn = DriverManager.
                getConnection("jdbc:ojp_h2:~/test", "sa", "");

        this.executeUpdate(conn,
                """
                drop table test_table
                """);

        this.executeUpdate(conn,
                """
                create table test_table(
                         id INT NOT NULL,
                           title VARCHAR(50) NOT NULL)
                """);

        this.executeUpdate(conn,
                """
                    insert into test_table (id, title) values (1, 'TITLE_1')
                    """
        );

        java.sql.PreparedStatement psSelect = conn.prepareStatement("select * from test_table where id = ?");
        psSelect.setInt(1, 1);
        ResultSet resultSet = psSelect.executeQuery();
        resultSet.next();
        int id = resultSet.getInt(1);
        String title = resultSet.getString(2);
        Assert.assertEquals(1, id);
        Assert.assertEquals("TITLE_1", title);

        executeUpdate(conn,
                """
                    update test_table set title='TITLE_1_UPDATED'
                    """
        );

        ResultSet resultSetUpdated = psSelect.executeQuery();
        resultSetUpdated.next();
        int idUpdated = resultSetUpdated.getInt(1);
        String titleUpdated = resultSetUpdated.getString(2);
        Assert.assertEquals(1, idUpdated);
        Assert.assertEquals("TITLE_1_UPDATED", titleUpdated);

        executeUpdate(conn,
                """
                    delete from test_table where id=1 and title='TITLE_1_UPDATED'
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
