package org.openjdbcproxy.jdbc;

import com.google.common.util.concurrent.SettableFuture;
import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import io.grpc.StatusRuntimeException;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.openjdbcproxy.grpc.client.StatementService;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.openjdbcproxy.constants.CommonConstants.MAX_LOB_DATA_BLOCK_SIZE;
import static org.openjdbcproxy.grpc.client.GrpcExceptionHandler.handle;

public class Blob implements java.sql.Blob {
    private final Connection connection;
    private final LobService lobService;
    private final StatementService statementService;
    private final SettableFuture<LobReference> lobReference = SettableFuture.create();

    public Blob(Connection connection, LobService lobService, StatementService statementService, LobReference lobReference) {
        this.connection = connection;
        this.lobService = lobService;
        this.statementService = statementService;
        if (lobReference != null) {
            this.lobReference.set(lobReference);
        }
    }

    public String getUUID() {
        try {
            return (this.lobReference != null) ? this.lobReference.get().getUuid() : null;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);//TODO review
        } catch (ExecutionException e) {
            throw new RuntimeException(e);//TODO review
        }
    }

    @Override
    public long length() throws SQLException {
        return 0;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            this.haveBlobReferenceValidation();
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
                            dataBlocks = statementService.readLob(lobReference.get(), currentPos, TWO_BLOCKS_SIZE);
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
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);//TODO review
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);//TODO review
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
            throw new RuntimeException(e);//TODO review
        }
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        return 0;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        try {
            //connect the pipes. Makes the OutputStream written by the caller feed into the InputStream read by the sender.
            PipedInputStream in = new PipedInputStream();
            PipedOutputStream out = new PipedOutputStream(in);

            CompletableFuture.supplyAsync(() -> {
                try {
                    this.lobReference.set(this.lobService.sendBytes(pos, in));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                //Refresh Session object.
                try {
                    this.connection.setSession(this.lobReference.get().getSession());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);//TODO review
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);//TODO review
                }
                return null;
            });

            return out;
        } catch (Exception e) {
            e.printStackTrace();//TODO treat exception
            throw new RuntimeException(e);
        }
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
