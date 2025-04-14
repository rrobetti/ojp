package org.openjdbcproxy.grpc.server;

import com.google.common.util.concurrent.SettableFuture;
import com.openjdbcproxy.grpc.LobDataBlock;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Accumulates LobDataBlocks and provide an implementation of InputStream interface, specially resolving the problem of
 * waiting for new blocks while feeding the InputStream at the same time.
 */
public class LobDataBlocksInputStream extends InputStream {
    private final List<LobDataBlock> blocksReceived;
    private byte[] currentBlock;
    private int currentIdx;
    private AtomicBoolean atomicFinished;
    private SettableFuture<Boolean> blockArrived;

    public LobDataBlocksInputStream(LobDataBlock firstBlock) {
        blocksReceived = Collections.synchronizedList(new ArrayList<>());
        this.currentBlock = firstBlock.getData().toByteArray();
        this.atomicFinished = new AtomicBoolean(false);
        this.blockArrived = SettableFuture.create();
        this.currentIdx = -1;
        this.blockArrived.set(true);
    }

    @SneakyThrows
    @Override
    public int read() {
        if (this.currentIdx >= this.currentBlock.length - 1) {
            if (this.blocksReceived.isEmpty()) {
                if (this.atomicFinished.get()) {//TODO add end current block exausted and no more blocks in the blocks received list.
                    return -1;//End of stream.
                }
                this.blockArrived.get(); //Wait for next block to arrive
                synchronized (this) {
                    this.blockArrived = SettableFuture.create();
                }
            }
            LobDataBlock nextBlock = this.blocksReceived.removeFirst();
            this.currentBlock = nextBlock.toByteArray();
            this.currentIdx = -1;
        }


        return this.currentBlock[++currentIdx] & 0xFF;
    }

    public void addBlock(LobDataBlock lobDataBlock) {
        this.blocksReceived.add(lobDataBlock);
        synchronized (this) {
            this.blockArrived.set(true);
        }
    }

    public void finish(boolean finished) {
        atomicFinished.set(finished);
    }
}
