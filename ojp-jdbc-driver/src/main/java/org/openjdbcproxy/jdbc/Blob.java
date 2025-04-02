package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import io.grpc.StatusRuntimeException;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.openjdbcproxy.grpc.client.StatementService;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Iterator;

import static org.openjdbcproxy.constants.CommonConstants.MAX_LOB_DATA_BLOCK_SIZE;
import static org.openjdbcproxy.grpc.client.GrpcExceptionHandler.handle;

@RequiredArgsConstructor
@AllArgsConstructor
public class Blob implements java.sql.Blob {
    private final Connection connection;
    private final LobService lobService;
    private final StatementService statementService;
    private LobReference lobReference = null;

    public String getUUID() {
        return (this.lobReference != null) ? this.lobReference.getUuid() : null;
    }

    @Override
    public long length() throws SQLException {
        return 0;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            this.haveBlobReferenceValidation();
            Iterator<LobDataBlock> dataBlocks = this.statementService.readLob(this.lobReference, pos, length);
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

    private void haveBlobReferenceValidation() throws SQLException {
        if (this.lobReference == null) {
            throw new SQLException("No reference to a LOB object found.");
        }
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        try {
            this.haveBlobReferenceValidation();

            return new InputStream() {
                private InputStream currentBlockInputStream;
                private int currentByte;
                private long currentPos = 0;
                private final int TWO_BLOCKS_SIZE = 2 * MAX_LOB_DATA_BLOCK_SIZE;

                @Override
                public int read() throws IOException {
                    currentByte = this.currentBlockInputStream != null? this.currentBlockInputStream.read() : -1;
                    currentPos++;
                    boolean lastBlockReached = (currentByte == -1 && currentPos > 1 && currentPos % TWO_BLOCKS_SIZE != 0);

                    if ((currentBlockInputStream == null || currentByte == -1) && !lastBlockReached) {
                        //Read next 2 blocks
                        Iterator<LobDataBlock> dataBlocks = null;
                        try {
                            dataBlocks = statementService.readLob(lobReference, currentPos, TWO_BLOCKS_SIZE);
                            this.currentBlockInputStream = lobService.parseReceivedBlocks(dataBlocks);
                            currentByte = this.currentBlockInputStream.read();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        } catch (StatusRuntimeException e) {
                            try {
                                throw handle(e);
                            } catch (SQLException ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    }

                    return currentByte;
                }
            };
        } catch (SQLException e) {
            throw e;
        } catch (StatusRuntimeException e) {
            throw handle(e);
        } catch (Exception e) {
            throw new SQLException("Unable to read all bytes from LOB object: " + e.getMessage(), e);
        }
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
        this.lobReference = this.lobService.sendBytes(pos, is);
        //Refresh Session object.
        this.connection.setSession(this.lobReference.getSession());
        return lobReference.getBytesWritten();
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        return 0;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return null;
    }

    @Override
    public void truncate(long len) throws SQLException {

    }

    @Override
    public void free() throws SQLException {

    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return null;
    }
}
