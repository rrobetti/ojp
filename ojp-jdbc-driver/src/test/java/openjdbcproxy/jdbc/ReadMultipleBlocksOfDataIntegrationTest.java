package openjdbcproxy.jdbc;

import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static openjdbcproxy.helpers.SqlHelper.executeUpdate;

public class ReadMultipleBlocksOfDataIntegrationTest {

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_postgres_connections_with_record_counts.csv")
    public void multiplePagesOfRowsResultSetSuccessful(int totalRecords, String driverClass, String url, String user, String pwd) throws SQLException, ClassNotFoundException {
        Class.forName(driverClass);
        Connection conn = DriverManager.getConnection(url, user, pwd);

        System.out.println("Testing retrieving " + totalRecords + " records from url -> " + url);

        try {
            executeUpdate(conn, "drop table test_table_multi");
        } catch (Exception e) {
            //Does not matter
        }
        executeUpdate(conn,
                "create table test_table_multi(" +
                        "id INT NOT NULL, " +
                        "title VARCHAR(50) NOT NULL)"
        );

        for (int i = 0; i < totalRecords; i++) { //TODO make this test parameterized with multiple parameters
            executeUpdate(conn,
                    "insert into test_table_multi (id, title) values (" + i + ", 'TITLE_" + i + "')"
            );
        }

        java.sql.PreparedStatement psSelect = conn.prepareStatement("select * from test_table_multi order by id");
        ResultSet resultSet = psSelect.executeQuery();

        for (int i = 0; i < totalRecords; i++) {
            resultSet.next();
            int id = resultSet.getInt(1);
            String title = resultSet.getString(2);
            Assert.assertEquals(i, id);
            Assert.assertEquals("TITLE_" + i, title);
        }

        executeUpdate(conn, "delete from test_table_multi");

        ResultSet resultSetAfterDeletion = psSelect.executeQuery();
        Assert.assertFalse(resultSetAfterDeletion.next());

        conn.close();
    }
}
