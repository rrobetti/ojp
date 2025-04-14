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

public class PostgressBLOBIntegrationTest {

    @Test
    public void createAndReadingBLOBsSuccessful() throws SQLException, ClassNotFoundException, IOException {
        Class.forName("org.openjdbcproxy.jdbc.Driver");
        Connection conn = DriverManager.
                getConnection("jdbc:postgresql:", "", "");

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
                         val_blob1 BYTEA,
                         val_blob2 BYTEA
                )
                """);

        conn.setAutoCommit(false);

        PreparedStatement psInsert = conn.prepareStatement(
                """
                    insert into test_table_blob (val_blob1, val_blob2) values (?, ?)
                    """
        );

        String testString = "BLOB VIA INPUT STREAM";
        InputStream inputStream = new ByteArrayInputStream(testString.getBytes());
        psInsert.setBinaryStream(1 , inputStream);
        InputStream inputStream2 = new ByteArrayInputStream(testString.getBytes());
        psInsert.setBinaryStream(2, inputStream2, 5);
        psInsert.executeUpdate();

        conn.commit();

        PreparedStatement psSelect = conn.prepareStatement("select val_blob1, val_blob2 from test_table_blob ");
        ResultSet resultSet = psSelect.executeQuery();
        resultSet.next();
        InputStream blobResult =  resultSet.getBinaryStream(1);
        String fromBlobByIdx = new String(blobResult.readAllBytes());

        Assert.assertEquals(testString, fromBlobByIdx);

        InputStream blobResultByName =  resultSet.getBinaryStream("val_blob1");
        String fromBlobByName = new String(blobResultByName.readAllBytes());
        Assert.assertEquals(testString, fromBlobByName);

        InputStream blobResult2 =  resultSet.getBinaryStream(1);
        String fromBlobByIdx2 = new String(blobResult2.readAllBytes());
        Assert.assertEquals(testString, fromBlobByIdx2);

        InputStream blobResult3 =  resultSet.getBinaryStream(2);
        String fromBlobByIdx3 = new String(blobResult3.readAllBytes());
        Assert.assertEquals(testString.substring(0, 5), fromBlobByIdx3);

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
    public void createAndReadingLargeBLOBsSuccessful() throws SQLException, ClassNotFoundException, IOException {
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

        PreparedStatement psSelect = conn.prepareStatement("select val_blob from test_table_blob ");
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
