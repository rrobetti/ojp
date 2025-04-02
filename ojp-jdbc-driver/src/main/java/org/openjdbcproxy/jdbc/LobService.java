package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Iterator;

public interface LobService {
    LobReference sendBytes(long pos, InputStream is) throws SQLException;
    InputStream parseReceivedBlocks(Iterator<LobDataBlock> itBlocks);
}
