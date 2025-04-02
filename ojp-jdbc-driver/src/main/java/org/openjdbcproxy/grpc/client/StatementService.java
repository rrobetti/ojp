package org.openjdbcproxy.grpc.client;

import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.SessionInfo;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * Proxy Server interface to handle the Jdbc requests.
 */
public interface StatementService {

    /**
     * Open a new JDBC connection with the database if one does not yet exit.
     */
    SessionInfo connect(ConnectionDetails connectionDetails) throws SQLException;
    Integer executeUpdate(SessionInfo sessionInfo, String sql, List<Parameter> params) throws SQLException;
    Iterator<OpResult> executeQuery(SessionInfo sessionInfo, String sql, List<Parameter> params) throws SQLException;
    LobReference createLob(Iterator<LobDataBlock> lobDataBlock) throws SQLException;
    Iterator<LobDataBlock> readLob(LobReference lobReference, long pos, int length) throws SQLException;
}
