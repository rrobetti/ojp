package org.openjdbcproxy.grpc.client;

import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.OpContext;
import com.openjdbcproxy.grpc.StatementServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.sql.SQLException;

import static org.openjdbcproxy.grpc.client.GrpcExceptionHandler.*;

public class StatementGrpcClient {
    public static void main(String[] args) throws SQLException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        StatementServiceGrpc.StatementServiceBlockingStub stub
                = StatementServiceGrpc.newBlockingStub(channel);

        try {
            OpContext ctx = stub.connect(ConnectionDetails.newBuilder()
                    .setUrl("jdbc:ojp_h2:~/test")
                    .setUser("sa")
                    .setPassword("").build());
            ctx.getConnHash();
        } catch (StatusRuntimeException e) {
            handle(e);
        }
        channel.shutdown();
    }
}
