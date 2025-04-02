package org.openjdbcproxy.grpc.server;

import com.openjdbcproxy.grpc.SqlErrorResponse;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;

import java.sql.SQLException;

/**
 * Handles exceptions that need to be reported via GRPC.
 */
public class GrpcExceptionHandler {
    /**
     * Handles the reporting or SQLExceptions.
     * @param e SQLException
     * @param streamObserver target stream observer.
     * @param <T> Stream observer generic type.
     */
    public static <T> void sendSQLExceptionMetadata(SQLException e, StreamObserver<T> streamObserver) {
        SqlErrorResponse sqlErrorResponse = SqlErrorResponse.newBuilder()
                .setReason(e.getMessage())
                .setSqlState(e.getSQLState())
                .setVendorCode(e.getErrorCode()).build();
        Metadata metadata = new Metadata();
        Metadata.Key<SqlErrorResponse> errorResponseKey = ProtoUtils.keyForProto(SqlErrorResponse.getDefaultInstance());
        metadata.put(errorResponseKey, sqlErrorResponse);
        streamObserver.onError(Status.CANCELLED.asRuntimeException(metadata));
    }
}
