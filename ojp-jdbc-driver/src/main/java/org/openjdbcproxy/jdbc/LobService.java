package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Iterator;

public interface LobService {
    LobReference sendBytes(LobType lobType, long pos, InputStream is) throws SQLException;
    InputStream parseReceivedBlocks(Iterator<LobDataBlock> itBlocks);
}
