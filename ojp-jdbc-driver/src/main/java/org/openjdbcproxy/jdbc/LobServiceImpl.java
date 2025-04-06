package org.openjdbcproxy.jdbc;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.openjdbcproxy.grpc.client.StatementService;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.openjdbcproxy.constants.CommonConstants.MAX_LOB_DATA_BLOCK_SIZE;

@AllArgsConstructor
public class LobServiceImpl implements LobService {

    private Connection connection;
    private StatementService statementService;

    @Override
    public LobReference sendBytes(long pos, InputStream is) throws SQLException {

        BufferedInputStream bis = new BufferedInputStream(is);
        AtomicInteger transferredBytes = new AtomicInteger(0);

        Iterator<LobDataBlock> itLobDataBlocks = new Iterator<LobDataBlock>() {

            byte[] nextBytes = new byte[]{};

            @SneakyThrows
            @Override
            public synchronized boolean hasNext() {
                //Read one next byte to know if the stream of bytes finished.
                nextBytes = bis.readNBytes(1);
                return nextBytes.length > 0;
            }

            @SneakyThrows
            @Override
            public synchronized LobDataBlock next() {
                byte[] bytesRead;
                if (nextBytes.length > 0) {
                    //H2 does not support multiple writes to the same blob. All is written at once. H2 error = Feature not supported: "Allocate a new object to set its value." [50100-232]
                    if (DbInfo.isH2DB()) {
                        bytesRead = Bytes.concat(nextBytes, bis.readAllBytes());
                    } else {
                        //Concatenate the one byte already read in the hasNext method
                        bytesRead = Bytes.concat(nextBytes, bis.readNBytes(MAX_LOB_DATA_BLOCK_SIZE - 1));
                    }
                } else {
                    bytesRead = bis.readNBytes(MAX_LOB_DATA_BLOCK_SIZE);
                }
                transferredBytes.set(transferredBytes.get() + (MAX_LOB_DATA_BLOCK_SIZE));
                return LobDataBlock.newBuilder()
                        .setSession(connection.getSession())
                        .setPosition((transferredBytes.get() + pos) - MAX_LOB_DATA_BLOCK_SIZE)
                        .setData(ByteString.copyFrom(bytesRead))
                        .build();
            }
        };

        return this.statementService.createLob(this.connection, itLobDataBlocks);
    }

    @Override
    public InputStream parseReceivedBlocks(Iterator<LobDataBlock> itBlocks) {
        return new InputStream() {
            private int currentPos = -1;

            private byte[] currentBlock;

            @Override
            public int read() throws IOException {
                if (currentBlock == null || currentPos >= (currentBlock.length - 1)) {
                    if (!itBlocks.hasNext()) {
                        return -1;// -1 means end of the stream.
                    }
                    currentBlock = itBlocks.next().getData().toByteArray();
                    currentPos = -1;
                }
                return currentBlock[++currentPos];
            }
        };
    }
}
