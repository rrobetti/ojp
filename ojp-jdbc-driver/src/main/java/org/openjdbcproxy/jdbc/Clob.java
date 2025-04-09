package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;
import lombok.SneakyThrows;
import org.openjdbcproxy.grpc.client.StatementService;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

public class Clob extends Lob implements java.sql.Clob {

    public Clob(Connection connection, LobServiceImpl lobService, StatementService statementService, LobReference lobReference) {
        super(connection, lobService, statementService, lobReference);
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        BufferedInputStream bis = new BufferedInputStream(this.getBinaryStream(pos, length));
        try {
            return new String(bis.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return null;
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return null;
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        return 0;
    }

    @Override
    public long position(java.sql.Clob searchstr, long start) throws SQLException {
        return 0;
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return this.setString(pos, str, 0, str.length());
    }

    @SneakyThrows
    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {

        int writtenCount = 0;
        try (Writer writer = this.setCharacterStream(pos)) {
            for (int i  = offset; i < len; i++) {
                writer.write(str.charAt(i));
                writtenCount++;
            }
        }
        return writtenCount;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return this.setBynaryStream(LobType.LT_CLOB, pos);
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        OutputStream os = this.setBynaryStream(LobType.LT_CLOB, pos);
        return new OutputStreamWriter(os);
    }

    @Override
    public void truncate(long len) throws SQLException {

    }

    @Override
    public void free() throws SQLException {

    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new InputStreamReader(super.getBinaryStream(pos, length));
    }
}
