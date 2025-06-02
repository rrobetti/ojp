package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
import com.openjdbcproxy.grpc.ResourceType;
import com.openjdbcproxy.grpc.TargetCall;
import lombok.Getter;
import org.openjdbcproxy.grpc.client.StatementService;

import java.sql.SQLException;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;

public class Savepoint implements java.sql.Savepoint {

    @Getter
    private final String savepointUUID;
    private final StatementService statementService;
    private final Connection connection;

    public Savepoint(String savepointUUID, StatementService statementService, Connection connection) {
        this.savepointUUID = savepointUUID;
        this.statementService = statementService;
        this.connection = connection;
    }

    @Override
    public int getSavepointId() throws SQLException {
        return this.retrieveAttribute(CallType.CALL_GET, "SavepointId", String.class);
    }

    @Override
    public String getSavepointName() throws SQLException {
        return this.retrieveAttribute(CallType.CALL_GET, "SavepointName", String.class);
    }

    private <T> T retrieveAttribute(CallType callType, String attrName, Class returnType) throws SQLException {
        CallResourceRequest.Builder reqBuilder = CallResourceRequest.newBuilder()
                .setSession(this.connection.getSession())
                .setResourceType(ResourceType.RES_SAVEPOINT)
                .setResourceUUID(this.savepointUUID)
                .setTarget(
                        TargetCall.newBuilder()
                                .setCallType(callType)
                                .setResourceName(attrName)
                                .build()
                );
        CallResourceResponse response = this.statementService.callResource(reqBuilder.build());
        this.connection.setSession(response.getSession());

        return (T) deserialize(response.getValues().toByteArray(), returnType);
    }
}
