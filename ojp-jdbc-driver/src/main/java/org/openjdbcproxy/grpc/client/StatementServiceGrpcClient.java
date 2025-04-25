package org.openjdbcproxy.grpc.client;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.ConnectionDetails;
import com.openjdbcproxy.grpc.LobDataBlock;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.ReadLobRequest;
import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.SessionTerminationStatus;
import com.openjdbcproxy.grpc.StatementRequest;
import com.openjdbcproxy.grpc.StatementServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.openjdbcproxy.grpc.dto.Parameter;
import org.openjdbcproxy.jdbc.Connection;
import org.openjdbcproxy.jdbc.LobGrpcIterator;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.openjdbcproxy.grpc.SerializationHandler.serialize;
import static org.openjdbcproxy.grpc.client.GrpcExceptionHandler.handle;

/**
 * Interacts with the GRPC client stub and handles exceptions.
 */
@Slf4j
public class StatementServiceGrpcClient implements StatementService {

    private final StatementServiceGrpc.StatementServiceBlockingStub statemetServiceBlockingStub;
    private final StatementServiceGrpc.StatementServiceStub statemetServiceStub;

    public StatementServiceGrpcClient() {
        //Once channel is open it remains open and is shared among all requests.
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        this.statemetServiceBlockingStub = StatementServiceGrpc.newBlockingStub(channel);
        this.statemetServiceStub = StatementServiceGrpc.newStub(channel);
    }

    @Override
    public SessionInfo connect(ConnectionDetails connectionDetails) throws SQLException {
        try {
            return this.statemetServiceBlockingStub.connect(connectionDetails);
        } catch (StatusRuntimeException e) {
            throw handle(e);
        }
    }

    @Override
    public OpResult executeUpdate(SessionInfo sessionInfo, String sql, List<Parameter> params) throws SQLException {
        return this.executeUpdate(sessionInfo, sql, params, "");
    }

    @Override
    public OpResult executeUpdate(SessionInfo sessionInfo, String sql, List<Parameter> params, String preparedStatementUUID)
            throws SQLException {
        try {
            return this.statemetServiceBlockingStub.executeUpdate(StatementRequest.newBuilder()
                    .setSession(sessionInfo)
                    .setPreparedStatementUUID(preparedStatementUUID != null ? preparedStatementUUID : "")
                    .setSql(sql)
                    .setParameters(ByteString.copyFrom(serialize(params)))
                    .build());
        } catch (StatusRuntimeException e) {
            throw handle(e);
        }
    }

    @Override
    public Iterator<OpResult> executeQuery(SessionInfo sessionInfo, String sql, List<Parameter> params) throws SQLException {
        try {
            return this.statemetServiceBlockingStub.executeQuery(StatementRequest.newBuilder()
                    .setSession(sessionInfo).setSql(sql).setParameters(ByteString.copyFrom(serialize(params))).build());
        } catch (StatusRuntimeException e) {
            throw handle(e);
        }
    }

    @Override
    public LobReference createLob(Connection connection, Iterator<LobDataBlock> lobDataBlock) throws SQLException {
        try {
            log.info("Creating new lob");
            //Indicates that the server acquired a connection to the DB and wrote the first block successfully.
            SettableFuture<LobReference> sfFirstLobReference = SettableFuture.create();
            //Indicates that the server has finished writing the last block successfully.
            SettableFuture<LobReference> sfFinalLobReference = SettableFuture.create();

            StreamObserver<LobDataBlock> lobDataBlockStream = this.statemetServiceStub.createLob(
                    new ServerCallStreamObserver<>() {
                        private final AtomicBoolean abFirstResponseReceived = new AtomicBoolean(true);
                        private LobReference lobReference;

                        @Override
                        public boolean isCancelled() {
                            return false;
                        }

                        @Override
                        public void setOnCancelHandler(Runnable runnable) {

                        }

                        @Override
                        public void setCompression(String s) {

                        }

                        @Override
                        public boolean isReady() {
                            return false;
                        }

                        @Override
                        public void setOnReadyHandler(Runnable runnable) {

                        }

                        @Override
                        public void request(int i) {

                        }

                        @Override
                        public void setMessageCompression(boolean b) {

                        }

                        @Override
                        public void disableAutoInboundFlowControl() {

                        }

                        @Override
                        public void onNext(LobReference lobReference) {
                            log.info("Lob reference received");
                            if (this.abFirstResponseReceived.get()) {
                                sfFirstLobReference.set(lobReference);
                                log.info("First lob reference trigger");
                            }
                            this.lobReference = lobReference;
                            //Update connection session on first confirmation to get the session id if session is new.
                            connection.setSession(lobReference.getSession());
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            if (throwable instanceof StatusRuntimeException sre) {
                                try {
                                    handle(sre);//To convert to SQLException if possible
                                    sfFirstLobReference.setException(sre);
                                    sfFinalLobReference.setException(sre); //When conversion to SQLException not possible
                                } catch (SQLException e) {
                                    sfFirstLobReference.setException(e);
                                    sfFinalLobReference.setException(e);
                                }
                            } else {
                                sfFirstLobReference.setException(throwable);
                                sfFinalLobReference.setException(throwable);
                            }
                        }

                        @Override
                        public void onCompleted() {
                            log.info("Final lob reference received");
                            sfFinalLobReference.set(this.lobReference);
                            log.info("Final lob reference notified");
                        }
                    }
            );

            //Send all data blocks one by one only after server finished consuming the previous block
            boolean firstBlockProcessedSuccessfully = false;
            while (lobDataBlock.hasNext()) {
                lobDataBlockStream.onNext(lobDataBlock.next());
                if (!firstBlockProcessedSuccessfully) {
                    //Wait first block to be processed by the server to avoid sending more data before the server actually acquired a connection and wrote the first block.
                    log.info("Waiting first lob reference arrival");
                    sfFirstLobReference.get();
                    log.info("First lob reference arrived");
                    firstBlockProcessedSuccessfully = true;
                }
            }
            lobDataBlockStream.onCompleted();

            log.info("Waiting for final lob ref");
            LobReference finalLobRef = sfFinalLobReference.get();
            log.info("Final lob ref received");
            return finalLobRef;
        } catch (StatusRuntimeException e) {
            throw handle(e);
        } catch (Exception e) {
            throw new SQLException("Unable to write LOB: " + e.getMessage(), e);
        }

    }

    @Override
    public Iterator<LobDataBlock> readLob(LobReference lobReference, long pos, int length) throws SQLException {
        try {
            LobGrpcIterator lobGrpcIterator = new LobGrpcIterator();
            SettableFuture<Boolean> sfFirstBlockReceived = SettableFuture.create();
            ReadLobRequest readLobRequest = ReadLobRequest.newBuilder()
                    .setLobReference(lobReference)
                    .setPosition(pos)
                    .setLength(length)
                    .build();

            final Throwable[] errorReceived = {null};

            this.statemetServiceStub.readLob(readLobRequest, new ServerCallStreamObserver<LobDataBlock>() {
                private final AtomicBoolean abFirstResponseReceived = new AtomicBoolean(true);

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public void setOnCancelHandler(Runnable runnable) {

                }

                @Override
                public void setCompression(String s) {

                }

                @Override
                public boolean isReady() {
                    return false;
                }

                @Override
                public void setOnReadyHandler(Runnable runnable) {

                }

                @Override
                public void request(int i) {

                }

                @Override
                public void setMessageCompression(boolean b) {

                }

                @Override
                public void disableAutoInboundFlowControl() {

                }

                @Override
                public void onNext(LobDataBlock lobDataBlock) {
                    lobGrpcIterator.addBlock(lobDataBlock);
                    if (abFirstResponseReceived.get()) {
                        sfFirstBlockReceived.set(true);
                        abFirstResponseReceived.set(false);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    errorReceived[0] = throwable;
                    lobGrpcIterator.setError(throwable);
                    sfFirstBlockReceived.set(false);
                }

                @Override
                public void onCompleted() {
                    lobGrpcIterator.finished();
                }
            });

            //Wait to receive at least one successful block before returning.
            if (!sfFirstBlockReceived.get() && errorReceived[0] != null) {
                if (errorReceived[0] instanceof Exception e) {
                    throw e;
                } else {
                    throw new RuntimeException(errorReceived[0]);
                }
            }


            return lobGrpcIterator;
        } catch (StatusRuntimeException e) {
            throw handle(e);
        } catch (Exception e) {
            throw new SQLException("Unable to write LOB: " + e.getMessage(), e);
        }
    }

    @Override
    public void terminateSession(SessionInfo session) {
        //Fire and forget - done async intentionally to improve client performance.
        this.statemetServiceStub.terminateSession(session, new ServerCallStreamObserver<>() {
            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public void setOnCancelHandler(Runnable runnable) {

            }

            @Override
            public void setCompression(String s) {

            }

            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setOnReadyHandler(Runnable runnable) {

            }

            @Override
            public void request(int i) {

            }

            @Override
            public void setMessageCompression(boolean b) {

            }

            @Override
            public void disableAutoInboundFlowControl() {

            }

            @Override
            public void onNext(SessionTerminationStatus sessionTerminationStatus) {
            }

            @Override
            public void onError(Throwable throwable) {
                Throwable t = throwable;
                if (throwable instanceof StatusRuntimeException sre) {
                    try {
                        handle(sre);
                    } catch (SQLException e) {
                        t = e;
                    }
                }
                log.error("Error while terminating session: " + t.getMessage(), t);
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    @Override
    public SessionInfo startTransaction(SessionInfo session) throws SQLException {
        try {
            return this.statemetServiceBlockingStub.startTransaction(session);
        } catch (StatusRuntimeException e) {
            throw handle(e);
        } catch (Exception e) {
            throw new SQLException("Unable to start a new transaction: " + e.getMessage(), e);
        }
    }

    @Override
    public SessionInfo commitTransaction(SessionInfo session) throws SQLException {
        try {
            return this.statemetServiceBlockingStub.commitTransaction(session);
        } catch (StatusRuntimeException e) {
            throw handle(e);
        } catch (Exception e) {
            throw new SQLException("Unable to commit transaction: " + e.getMessage(), e);
        }
    }

    @Override
    public SessionInfo rollbackTransaction(SessionInfo session) throws SQLException {
        try {
            return this.statemetServiceBlockingStub.rollbackTransaction(session);
        } catch (StatusRuntimeException e) {
            throw handle(e);
        } catch (Exception e) {
            throw new SQLException("Unable to rollback transaction: " + e.getMessage(), e);
        }
    }
}
