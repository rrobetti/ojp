package org.openjdbcproxy.grpc.server;

import lombok.AllArgsConstructor;

import java.sql.Connection;

//TODO Transaction is a Session and a Session will have transaction(s) sometimes one session has one transaction but one session might have multiple transactions
public class Transaction extends Session {
    public Transaction(Connection connection, String connectionHash, String clientUUID) {
        super(connection, connectionHash, clientUUID);
    }
}
