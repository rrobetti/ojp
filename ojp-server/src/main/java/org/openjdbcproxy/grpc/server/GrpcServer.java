package org.openjdbcproxy.grpc.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.openjdbcproxy.constants.CommonConstants;

import java.io.IOException;

public class GrpcServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder
                .forPort(CommonConstants.DEFAULT_PORT_NUMBER)
                .addService(new StatementServiceImpl(
                        new SessionManagerImpl(),
                        new CircuitBreaker(60000)//TODO pass as parameter currently
                )).build();

        server.start();
        server.awaitTermination();
    }
}
