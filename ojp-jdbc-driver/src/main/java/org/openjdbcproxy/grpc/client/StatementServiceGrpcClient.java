package org.openjdbcproxy.grpc.client;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.OpContext;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.StatementRequest;
import com.openjdbcproxy.grpc.StatementServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.openjdbcproxy.grpc.dto.OpQueryResult;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.openjdbcproxy.grpc.SerializationHandler.serialize;
import static org.openjdbcproxy.grpc.client.GrpcExceptionHandler.handle;
import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;

/**
 * Interacts with the GRPC client stub and handles exceptions.
 */
public class StatementServiceGrpcClient implements StatementService {

    private StatementServiceGrpc.StatementServiceBlockingStub statemetServiceStub;
    private ManagedChannel channel = null;

    public StatementServiceGrpcClient() {
        channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        this.statemetServiceStub = StatementServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public OpContext connect(ConnectionDetails connectionDetails) throws SQLException {
        try {
            return this.statemetServiceStub.connect(connectionDetails);
        } catch (StatusRuntimeException e) {
            throw handle(e);
        }
    }

    @Override
    public Integer executeUpdate(OpContext ctx, String sql, List<Parameter> params) throws SQLException {
        try {
            OpResult result = this.statemetServiceStub.executeUpdate(StatementRequest.newBuilder()
                    .setContext(ctx).setSql(sql).setParameters(ByteString.copyFrom(serialize(params))).build());
            return deserialize(result.getValue().toByteArray(), Integer.class);
        } catch (StatusRuntimeException e) {
            throw handle(e);
        }
    }

    @Override
    public OpQueryResult executeQuery(OpContext ctx, String sql, List<Parameter> params) throws SQLException {
        try {
            OpResult result = this.statemetServiceStub.executeQuery(StatementRequest.newBuilder()
                    .setContext(ctx).setSql(sql).setParameters(ByteString.copyFrom(serialize(params))).build());
            return deserialize(result.getValue().toByteArray(), OpQueryResult.class);
        } catch (StatusRuntimeException e) {
            throw handle(e);
        }
    }

    @Override
    public void close() {
        try {
            this.channel.shutdown();
            this.channel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
