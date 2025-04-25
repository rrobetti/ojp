package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;
import io.grpc.StatusRuntimeException;
import org.openjdbcproxy.grpc.client.StatementService;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Iterator;

import static org.openjdbcproxy.grpc.client.GrpcExceptionHandler.handle;

public class Blob extends Lob implements java.sql.Blob {
    public Blob(Connection connection, LobService lobService, StatementService statementService, LobReference lobReference) {
        super(connection, lobService, statementService, lobReference);
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            this.haveLobReferenceValidation();
            Iterator<LobDataBlock> dataBlocks = this.statementService.readLob(this.lobReference.get(), pos, length);
            InputStream is = this.lobService.parseReceivedBlocks(dataBlocks);
            BufferedInputStream bis = new BufferedInputStream(is);
            return bis.readAllBytes();
        } catch (SQLException e) {
            throw e;
        } catch (StatusRuntimeException e) {
            throw handle(e);
        } catch (Exception e) {
            throw new SQLException("Unable to read all bytes from LOB object: " + e.getMessage(), e);
        }
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return super.getBinaryStream(1, Long.MAX_VALUE);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        return 0;
    }

    @Override
    public long position(java.sql.Blob pattern, long start) throws SQLException {
        return 0;
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        InputStream is = new ByteArrayInputStream(bytes);
        OutputStream os = this.setBinaryStream(pos);
        int byteRead;
        int writtenCount = 0;
        try {
            while ((byteRead = is.read()) != -1) {
                os.write(byteRead);
                writtenCount++;
            }
            os.close();
            return writtenCount;
        } catch (IOException e) {
            throw new SQLException("Unable to write bytes: " + e.getMessage(), e);
        }
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        return 0;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return super.setBinaryStream(LobType.LT_BLOB, pos);
    }

    @Override
    public void truncate(long len) throws SQLException {

    }

    @Override
    public void free() throws SQLException {

    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return super.getBinaryStream(pos, length);
    }
}
