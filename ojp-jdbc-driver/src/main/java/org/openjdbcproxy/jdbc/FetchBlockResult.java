package org.openjdbcproxy.jdbc;

import lombok.Builder;
import lombok.Data;
import org.openjdbcproxy.grpc.dto.OpQueryResult;

import java.sql.SQLException;

@Builder
@Data
public class FetchBlockResult {
    private OpQueryResult result;
    private SQLException exception;
}
