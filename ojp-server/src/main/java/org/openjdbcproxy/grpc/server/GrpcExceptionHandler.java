package org.openjdbcproxy.grpc.server;

import com.openjdbcproxy.grpc.SqlErrorResponse;
import com.openjdbcproxy.grpc.SqlErrorType;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

/**
 * Handles exceptions that need to be reported via GRPC.
 */
@Slf4j
public class GrpcExceptionHandler {

    /**
     * Handles the reporting or SQLExceptions.
     * @param e SQLException
     * @param streamObserver target stream observer.
     * @param <T> Stream observer generic type.
     */
    public static <T> void sendSQLExceptionMetadata(SQLException e, StreamObserver<T> streamObserver) {
        sendSQLExceptionMetadata(e, streamObserver, SqlErrorType.SQL_EXCEPTION);
    }

        /**
         * Handles the reporting or SQLExceptions.
         * @param e SQLException
         * @param streamObserver target stream observer.
         * @param <T> Stream observer generic type.
         * @param sqlErrorType Indicates the type of error.
         */
    public static <T> void sendSQLExceptionMetadata(SQLException e, StreamObserver<T> streamObserver, SqlErrorType sqlErrorType) {

        Metadata metadata = new Metadata();
        try {
            SqlErrorResponse sqlErrorResponse = SqlErrorResponse.newBuilder()
                    .setReason(e.getMessage())
                    .setSqlState(e.getSQLState())
                    .setSqlErrorType(sqlErrorType)
                    .setVendorCode(e.getErrorCode()).build();
            Metadata.Key<SqlErrorResponse> errorResponseKey = ProtoUtils.keyForProto(SqlErrorResponse.getDefaultInstance());
            metadata.put(errorResponseKey, sqlErrorResponse);
        } catch (RuntimeException re) {
            log.error("Failed while sending error to client: " + re.getMessage() + ": " + e.getMessage(), e);
        }
        streamObserver.onError(Status.CANCELLED.asRuntimeException(metadata));
    }
}
