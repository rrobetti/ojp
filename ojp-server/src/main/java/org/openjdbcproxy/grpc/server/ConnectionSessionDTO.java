package org.openjdbcproxy.grpc.server;

import com.openjdbcproxy.grpc.SessionInfo;
import lombok.Builder;
import lombok.Getter;

import java.sql.Connection;

@Getter
@Builder
public class ConnectionSessionDTO {
    private Connection connection;
    private SessionInfo session;
}
