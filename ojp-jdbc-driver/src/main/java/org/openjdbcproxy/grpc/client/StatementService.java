package org.openjdbcproxy.grpc.client;

import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.OpContext;
import com.openjdbcproxy.grpc.OpResult;
import org.openjdbcproxy.grpc.dto.OpQueryResult;
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
    OpContext connect(ConnectionDetails connectionDetails) throws SQLException;
    Integer executeUpdate(OpContext ctx, String sql, List<Parameter> params) throws SQLException;
    Iterator<OpResult> executeQuery(OpContext ctx, String sql, List<Parameter> params) throws SQLException;
}
