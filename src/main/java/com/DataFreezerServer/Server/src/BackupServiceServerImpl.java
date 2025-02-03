package com.DataFreezerServer.Server.src;

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

public class BackupServiceServerImpl extends BackupServiceGrpc.BackupServiceImplBase {
    private final CqlSession cassandraSession;

    public BackupServiceServerImpl() {
        try {
            this.cassandraSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("localhost", 9042))
                    .withKeyspace("dbdatafreezer")
                    .withLocalDatacenter("datacenter1")
                    .build();
        } catch (Exception e) {
            System.out.println("Error al conectar a Cassandra: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void login(LoginRequest request, StreamObserver<LoginResponse> responseObserver) {
        String username = request.getUsername();
        String password = request.getPassword();

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
            } else {
                // Invalid credentials
                LoginResponse response = LoginResponse.newBuilder()
                        .setMessage("Invalid credentials")
                        .setSuccess(false)
                        .build();
                responseObserver.onNext(response);
            }
        } catch (Exception e) {
            System.err.println("Error during login: " + e.getMessage());
            responseObserver.onNext(LoginResponse.newBuilder()
                    .setMessage("Internal server error")
                    .setSuccess(false)
                    .build());
        } finally {
            responseObserver.onCompleted();
        }
    }
}
