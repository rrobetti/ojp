package openjdbcproxy.jdbc;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BLOBIntegrationTest {

    @Test
    public void creatinAndReadingBLOBsSuccessful() throws SQLException, ClassNotFoundException, IOException {
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
                    insert into test_table_blob (val_blob, val_blob2, val_blob3) values (?, ?, ?)
                    """
        );

        String testString = "TEST STRING BLOB";
        Blob blob = conn.createBlob(); //WHEN this happens a connection in the server is set to a session and I need to replicate that in the
        //preparaed statement created previously
        blob.setBytes(1, testString.getBytes());
        psInsert.setBlob(1, blob);
        String testString2 = "BLOB VIA INPUT STREAM";
        InputStream inputStream = new ByteArrayInputStream(testString2.getBytes());
        psInsert.setBlob(2 , inputStream);
        InputStream inputStream2 = new ByteArrayInputStream(testString2.getBytes());
        psInsert.setBlob(3, inputStream2, 5);
        psInsert.executeUpdate();

        java.sql.PreparedStatement psSelect = conn.prepareStatement("select val_blob, val_blob2, val_blob3 from test_table_blob ");
        ResultSet resultSet = psSelect.executeQuery();
        resultSet.next();
        Blob blobResult =  resultSet.getBlob(1);
        String fromBlobByIdx = new String(blobResult.getBinaryStream().readAllBytes());

        Assert.assertEquals(testString, fromBlobByIdx);

        Blob blobResultByName =  resultSet.getBlob("val_blob");
        String fromBlobByName = new String(blobResultByName.getBinaryStream().readAllBytes());
        Assert.assertEquals(testString, fromBlobByName);

        Blob blobResult2 =  resultSet.getBlob(2);
        String fromBlobByIdx2 = new String(blobResult2.getBinaryStream().readAllBytes());
        Assert.assertEquals(testString2, fromBlobByIdx2);

        Blob blobResult3 =  resultSet.getBlob(3);
        String fromBlobByIdx3 = new String(blobResult3.getBinaryStream().readAllBytes());
        Assert.assertEquals(testString2.substring(0, 5), fromBlobByIdx3);

        executeUpdate(conn,
                """
                    delete from test_table_blob
                    """
        );

        resultSet.close();
        psSelect.close();
        conn.close();
    }

    @Test
    public void creatinAndReadingLargeBLOBsSuccessful() throws SQLException, ClassNotFoundException, IOException {
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
                         val_blob  BLOB
                )
                """);

        PreparedStatement psInsert = conn.prepareStatement(
                """
                    insert into test_table_blob (val_blob) values (?)
                    """
        );


        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("largeTextFile.txt");
        psInsert.setBlob(1 , inputStream);

        psInsert.executeUpdate();

        java.sql.PreparedStatement psSelect = conn.prepareStatement("select val_blob from test_table_blob ");
        ResultSet resultSet = psSelect.executeQuery();
        resultSet.next();
        Blob blobResult =  resultSet.getBlob(1);

        InputStream inputStreamTestFile = this.getClass().getClassLoader().getResourceAsStream("largeTextFile.txt");
        InputStream inputStreamBlob = blobResult.getBinaryStream();

        int byteFile = inputStreamTestFile.read();
        int count = 0;
        while (byteFile != -1) {
            count++;
            if (count == 3072) {
                System.out.println(count);
            }
            int blobByte = inputStreamBlob.read();
            if (byteFile != blobByte) {
                System.out.println(count);
            }

            Assert.assertEquals(byteFile, blobByte);
            byteFile = inputStreamTestFile.read();
        }

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
