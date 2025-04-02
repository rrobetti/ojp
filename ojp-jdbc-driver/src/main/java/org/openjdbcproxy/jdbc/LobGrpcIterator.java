package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.LobDataBlock;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator over the blocks received when getting a LOB object.
 * IMPORTANT: can only be consumed once as per the data is removed from memory once consumed.
 */
public class LobGrpcIterator implements Iterator<LobDataBlock> {
    private List<LobDataBlock> blocksReceived = Collections.synchronizedList(new ArrayList<>());
    @Setter
    private Throwable error;

    public void addBlock(LobDataBlock block) {
        this.blocksReceived.add(block);
    }

    @Override
    public boolean hasNext() {
        if (this.error != null) {
            throw new RuntimeException(this.error);
        }
        return blocksReceived.size() > 0;
    }

    @Override
    public LobDataBlock next() {
        if (this.error != null) {
            throw new RuntimeException(this.error);
        }
        LobDataBlock block = this.blocksReceived.getFirst();
        this.blocksReceived.removeFirst();
        return block;
    }
}
