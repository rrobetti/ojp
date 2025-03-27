package org.openjdbcproxy.grpc.client;

import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.OpContext;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.ResultSetId;
import org.openjdbcproxy.grpc.dto.OpQueryResult;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.io.Closeable;
import java.sql.SQLException;
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
    OpQueryResult executeQuery(OpContext ctx, String sql, List<Parameter> params) throws SQLException;
    OpQueryResult readResultSetData(String resultSetUUID) throws SQLException;
}
