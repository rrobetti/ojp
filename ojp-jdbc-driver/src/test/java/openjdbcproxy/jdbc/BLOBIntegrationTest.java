package openjdbcproxy.jdbc;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.openjdbcproxy.jdbc.Constants;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BLOBIntegrationTest {

    @Test
    public void crudBLOBSuccessful() throws SQLException, ClassNotFoundException, IOException {
        /*Class.forName(Constants.H2_DRIVER_CLASS);
        Connection conn = DriverManager.
                getConnection("jdbc:h2:~/test", "sa", "");
        */
        Class.forName("org.openjdbcproxy.jdbc.Driver");
        Connection conn = DriverManager.
                getConnection("jdbc:ojp_h2:~/test", "sa", "");

        try {
            this.executeUpdate(conn,
                    """
                            drop table test_table_blob
                            """);
        } catch (Exception e) {
            //If fails disregard as per the table is most possibly not created yet
        }

        this.executeUpdate(conn,
                """
                create table test_table_blob(
                         val_blob  BLOB,
                         val_blob2 BLOB,
                         val_blob3 BLOB
                )
                """);

        PreparedStatement psInsert = conn.prepareStatement(
                """
                    insert into test_table_blob (val_blob) values (?)
                    """
        );

        String testString = "TEST STRING BLOB";
        Blob blob = conn.createBlob(); //WHEN this happens a connection in the server is set to a session and I need to replicate that in the
        //preparaed statement created previously
        blob.setBytes(1, testString.getBytes());
        psInsert.setBlob(1, blob);
        //TODO test blobs with Input stream as well
        psInsert.executeUpdate();

        java.sql.PreparedStatement psSelect = conn.prepareStatement("select val_blob from test_table_blob ");
        ResultSet resultSet = psSelect.executeQuery();
        resultSet.next();
        //TODO also get blob by col name
        Blob blobResult =  resultSet.getBlob(1);
        //blobResult.free();
        //TODO add multiple ways of getting bytes, by sending position and lengh for example
        String fromBlobByIdx = new String(blobResult.getBinaryStream().readAllBytes());

        //TODO add tests for oder operations like getting the pos by sending a partial array of bytes and start pos
        Assert.assertEquals(testString, fromBlobByIdx);

        executeUpdate(conn,
                """
                    delete from test_table_blob
                    """
        );

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
