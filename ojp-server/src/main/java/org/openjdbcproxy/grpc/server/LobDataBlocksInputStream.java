package org.openjdbcproxy.grpc.server;

import com.google.common.util.concurrent.SettableFuture;
import com.openjdbcproxy.grpc.LobDataBlock;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Accumulates LobDataBlocks and provide an implementation of InputStream interface, specially resolving the problem of
 * waiting for new blocks while feeding the InputStream at the same time.
 */
@Slf4j
public class LobDataBlocksInputStream extends InputStream {
    @Getter
    private final String uuid;
    private final List<LobDataBlock> blocksReceived;
    private final AtomicBoolean atomicFinished;
    private byte[] currentBlock;
    private int currentIdx;
    private SettableFuture<Boolean> blockArrived;
    @Getter
    private AtomicBoolean fullyConsumed;

    public LobDataBlocksInputStream(LobDataBlock firstBlock) {
        this.uuid = UUID.randomUUID().toString();
        this.fullyConsumed = new AtomicBoolean(false);
        this.blocksReceived = new ArrayList<>();
        this.currentBlock = firstBlock.getData().toByteArray();
        this.atomicFinished = new AtomicBoolean(false);
        this.blockArrived = SettableFuture.create();
        this.currentIdx = -1;
        this.blockArrived.set(true);
        log.info("{} lob created", this.uuid);
    }

    @SneakyThrows
    @Override
    public int read() {
        //TODO remove
        log.info("Reading lob {}", this.uuid);
        if (this.currentIdx >= this.currentBlock.length - 1) {
            log.info("Current block has no bytes to read.");
            if (this.blocksReceived.isEmpty()) {
                log.info("No new blocks received, will wait for block to arrive if stream not finished");
                Thread.sleep(10);//TODO review if possible to remove, here because it was reading the flag beffore blocks feeder thread had a chance to mark it as finished.
                if (this.atomicFinished.get()) {
                    log.info("All blocks exhausted, finishing byte stream. lob {}", this.uuid);
                    this.fullyConsumed.set(true);
                    return -1;//End of stream.
                }
                try {
                    this.blockArrived.get(5, TimeUnit.SECONDS); //Wait for next block to arrive
                } catch (TimeoutException e) {
                    if (this.atomicFinished.get()) {
                        log.info("All blocks exhausted, timed out waiting for new block, finishing byte stream. lob {}", this.uuid);
                        this.fullyConsumed.set(true);
                        return -1;//End of stream.
                    }
                }
                log.info("New block arrived");
                synchronized (this) {
                    this.blockArrived = SettableFuture.create();
                }
            }
            synchronized (this) {
                LobDataBlock nextBlock = this.blocksReceived.removeFirst();
                this.currentBlock = nextBlock.getData().toByteArray();
            }
            this.currentIdx = -1;
            log.info("Nex block positioned for reading");
        }

        int ret = this.currentBlock[++currentIdx];
        return ret & 0xFF;
    }

    public void addBlock(LobDataBlock lobDataBlock) {
        synchronized (this) {
            this.blocksReceived.add(lobDataBlock);
            this.blockArrived.set(true);
        }
    }

    /**
     * Indicate that it finished receiving blocks not that if finished being read.
     *
     * @param finished
     */
    public void finish(boolean finished) {
        log.info("Finished receiving blocks");
        atomicFinished.set(finished);
    }
}
