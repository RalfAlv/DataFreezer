package com.DataFreezerServer.Server.src;

import com.common.PropertiesManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.proto.backupservice.BackupServiceGrpc;
import com.proto.backupservice.LoginRequest;
import com.proto.backupservice.LoginResponse;
import io.grpc.stub.StreamObserver;

import java.net.InetSocketAddress;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupServiceServerImpl extends BackupServiceGrpc.BackupServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(BackupServiceServer.class);
    private final CqlSession cassandraSession;

    private static final  String serverHost = PropertiesManager.getInstance().getProperty("main.host");
    private static final int portCassandra = PropertiesManager.getInstance().getIntProperty("cassandra.port");

    public BackupServiceServerImpl() {
        try {
            logger.info("Attempting to connect to Cassandra...");
            this.cassandraSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(serverHost, portCassandra))
                    .withKeyspace("dbdatafreezer")
                    .withLocalDatacenter("datacenter1")
                    .build();
            logger.info("Cassandra connection established successfully.");
        } catch (Exception e) {
            logger.error("Error connecting to Cassandra: {}", e.getMessage());
            throw e;
        }
    }


    @Override
    public void login(LoginRequest request, StreamObserver<LoginResponse> responseObserver) {
        String username = request.getUsername();
        String password = request.getPassword();

        logger.debug("Received login request for user: {}", username);

        try {
            // Declaration prepared for greater security and performance
            PreparedStatement prepared = cassandraSession.prepare(
                    "SELECT user_id FROM users WHERE username = ? AND password = ? ALLOW FILTERING"
            );

            BoundStatement bound = prepared.bind(username, password);
            ResultSet resultSet = cassandraSession.execute(bound);
            Row row = resultSet.one();

            if (row != null) {
                // Generate session token
                String sessionToken = UUID.randomUUID().toString();
                long expirationTime = System.currentTimeMillis() + (60 * 60 * 1000); // 1 hr
                // Insert the token into the sessions table
                cassandraSession.execute(
                        "INSERT INTO session_tokens (username, \"token\", expiration_time, created_at) VALUES (?, ?, ?, toTimestamp(now()))",
                        username, sessionToken, expirationTime
                );

                // Success response
                LoginResponse response = LoginResponse.newBuilder()
                        .setSessionToken(sessionToken)
                        .setMessage("Login successful")
                        .setSuccess(true)
                        .build();
                responseObserver.onNext(response);
                logger.info("Login successful for user: {}. Session Token: {}", username, sessionToken);
            } else {
                // Invalid credentials
                LoginResponse response = LoginResponse.newBuilder()
                        .setMessage("Invalid credentials")
                        .setSuccess(false)
                        .build();
                responseObserver.onNext(response);
                logger.warn("Login failed for user: {}. Invalid credentials.", username);
            }
        } catch (Exception e) {
            logger.error("Error during login for user: {}: {}", username, e.getMessage());
            responseObserver.onNext(LoginResponse.newBuilder()
                    .setMessage("Internal server error")
                    .setSuccess(false)
                    .build());
        } finally {
            responseObserver.onCompleted();
        }
    }
}
