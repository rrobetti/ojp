package org.openjdbcproxy.grpc.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConnectionDetails {
    private String url;
    private String user;
    private String password;
}
