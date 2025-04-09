package org.openjdbcproxy.jdbc;

import com.google.common.util.concurrent.SettableFuture;
import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;
import io.grpc.StatusRuntimeException;
import org.openjdbcproxy.grpc.client.StatementService;

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

public class Lob {
    protected final Connection connection;
    protected final LobService lobService;
    protected final StatementService statementService;
    protected final SettableFuture<LobReference> lobReference = SettableFuture.create();

    public Lob(Connection connection, LobService lobService, StatementService statementService, LobReference lobReference) {
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

    public long length() throws SQLException {
        return 0; //TODO implement
    }

    protected OutputStream setBynaryStream(LobType lobType, long pos) {
        try {
            //connect the pipes. Makes the OutputStream written by the caller feed into the InputStream read by the sender.
            PipedInputStream in = new PipedInputStream();
            PipedOutputStream out = new PipedOutputStream(in);

            CompletableFuture.supplyAsync(() -> {
                try {
                    this.lobReference.set(this.lobService.sendBytes(lobType, pos, in));
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

    protected void haveBlobReferenceValidation() throws SQLException {
        if (this.lobReference == null) {
            throw new SQLException("No reference to a LOB object found.");
        }
    }

    protected InputStream getBinaryStream(long pos, long length) throws SQLException {
        try {
            this.haveBlobReferenceValidation();

            return new InputStream() {
                private InputStream currentBlockInputStream;
                private long currentPos = pos - 1;//minus 1 because it will increment it in the loop

                @Override
                public int read() throws IOException {
                    int currentByte = this.currentBlockInputStream != null ? this.currentBlockInputStream.read() : -1;
                    int TWO_BLOCKS_SIZE = 2 * MAX_LOB_DATA_BLOCK_SIZE;
                    boolean lastBlockReached = (currentByte == -1 && currentPos > 1 && currentPos % TWO_BLOCKS_SIZE != 0);
                    currentPos++;

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

                    if (currentPos >= length) {
                        return -1;//Finish stream if reached the length required
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
}
