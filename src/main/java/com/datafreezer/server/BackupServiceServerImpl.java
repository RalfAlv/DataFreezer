package com.datafreezer.server;

import com.datafreezer.common.PropertiesManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.protobuf.ByteString;
import com.proto.backupservice.BackupServiceGrpc;
import com.proto.backupservice.LoginRequest;
import com.proto.backupservice.LoginResponse;
import com.proto.backupservice.UploadFileRequest;
import com.proto.backupservice.UploadFileResponse;
import com.proto.backupservice.ListFileRequest;
import com.proto.backupservice.ListFileResponse;
import com.proto.backupservice.DeleteFileRequest;
import com.proto.backupservice.DeleteFileResponse;
import com.proto.backupservice.DownloadFileRequest;
import com.proto.backupservice.DownloadFileResponse;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupServiceServerImpl extends BackupServiceGrpc.BackupServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(BackupServiceServer.class);
    private final CqlSession cassandraSession;

    private static final String serverHost = PropertiesManager.getInstance().getProperty("main.host");
    private static final int portCassandra = PropertiesManager.getInstance().getIntProperty("cassandra.port");
    private static final String Files_dir = PropertiesManager.getInstance().getProperty("storage.path");
    //private static final String Files_dir = "src/main/java/com/common/files";

    //private static final int chunkZise = PropertiesManager.getInstance().getIntProperty("chunk.size");
    private static final int chunk_size = 1024 * 1024;

    public BackupServiceServerImpl() {
        try {
            logger.info("Attempting to connect to Cassandra...");
            this.cassandraSession = CqlSession.builder().addContactPoint(new InetSocketAddress(serverHost, portCassandra)).withKeyspace("dbdatafreezer").withLocalDatacenter("datacenter1").build();
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
            PreparedStatement prepared = cassandraSession.prepare("SELECT user_id FROM users WHERE username = ? AND password = ? ALLOW FILTERING");

            BoundStatement bound = prepared.bind(username, password);
            ResultSet resultSet = cassandraSession.execute(bound);
            Row row = resultSet.one();

            if (row != null) {
                // Generate session token
                String sessionToken = UUID.randomUUID().toString();
                long expirationTime = System.currentTimeMillis() + (60 * 60 * 1000); // 1 hr
                // Insert the token into the sessions table
                cassandraSession.execute("INSERT INTO session_tokens (username, \"token\", expiration_time, created_at) VALUES (?, ?, ?, toTimestamp(now()))", username, sessionToken, expirationTime);

                // Success response
                LoginResponse response = LoginResponse.newBuilder().setSessionToken(sessionToken).setMessage("Login successful").setSuccess(true).build();
                responseObserver.onNext(response);
                logger.info("Login successful for user: {}. Session Token: {}", username, sessionToken);
            } else {
                // Invalid credentials
                LoginResponse response = LoginResponse.newBuilder().setMessage("Invalid credentials").setSuccess(false).build();
                responseObserver.onNext(response);
                logger.warn("Login failed for user: {}. Invalid credentials.", username);
            }
        } catch (Exception e) {
            logger.error("Error during login for user: {}: {}", username, e.getMessage());
            responseObserver.onNext(LoginResponse.newBuilder().setMessage("Internal server error").setSuccess(false).build());
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<UploadFileRequest> uploadFile(StreamObserver<UploadFileResponse> responseObserver) {
        return new StreamObserver<>() {
            private String fileName;
            private UUID fileId;
            private RandomAccessFile raf;
            private long totalChunks;
            private long receivedChunks = 0;

            @Override
            public void onNext(UploadFileRequest request) {
                try {
                    if (raf == null) {
                        fileName = request.getFileName();
                        fileId = UUID.randomUUID();
                        totalChunks = request.getTotalChunks();

                        File file = new File(Files_dir, fileName);

                        int counter = 1;
                        while (file.exists()) {
                            String name = fileName;
                            int dotIndex = fileName.lastIndexOf('.');
                            if (dotIndex != -1) {
                                name = fileName.substring(0, dotIndex) + "_" + counter + fileName.substring(dotIndex);
                            } else {
                                name = fileName + "_" + counter;
                            }
                            file = new File(Files_dir, name);
                            counter++;
                        }
                        fileName = file.getName();
                        raf = new RandomAccessFile(file, "rw");
                        logger.info("Starting upload of file: {}", fileName);
                    }

                    //write chunk
                    raf.seek(request.getChunkNumber() * 1024 * 1024); //1MB
                    raf.write(request.getData().toByteArray());
                    receivedChunks++;

                    if (receivedChunks == totalChunks) {
                        completeUpload(request.getUsername());
                    }
                } catch (Exception err) {
                    logger.error("Error during upload file: {}", err.getMessage());
                }
            }

            private void completeUpload(String username) {
                try {
                    long fileSize = raf.length();
                    raf.close();

                    UUID userId = getUserId(username);
                    String filePath = Files_dir + File.separator + fileName;

                    // Insert file metadata
                    cassandraSession.execute(
                            "INSERT INTO files (file_id, user_id, file_name, file_path_server, file_size, uploaded_at) " +
                                    "VALUES (?, ?, ?, ?, ?, toTimestamp(now()))",
                            fileId, userId, fileName, filePath, fileSize
                    );

                    logger.info("File upload completed: {}", fileName);

                    // Insert user action
                    UUID actionId = UUID.randomUUID();
                    cassandraSession.execute(
                            "INSERT INTO user_actions (action_id, user_id, action_type, file_id, action_timestamp, action_details) " +
                                    "VALUES (?, ?, ?, ?, toTimestamp(now()), ?)",
                            actionId, userId, "Upload", fileId, "File uploaded: " + fileName
                    );

                    //Send success response
                    responseObserver.onNext(UploadFileResponse.newBuilder()
                            .setSucess(true)
                            .setMessage("File uploaded successfully")
                            .setFileId(fileId.toString())
                            .build());
                    responseObserver.onCompleted();
                    logger.info("File upload completed. FileID: {}, User: {}", fileId, username);
                } catch (Exception err) {
                    logger.error("Error cleaning up: {}", err.getMessage());
                    responseObserver.onNext(UploadFileResponse.newBuilder()
                            .setSucess(false)
                            .setMessage("Upload failed: " + err.getMessage())
                            .build());
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("Upload error: {}", throwable.getMessage());
                try {
                    if (raf != null) {
                        raf.close();
                    }
                } catch (Exception err) {
                    logger.error("Error closing file: {}", err.getMessage());
                }
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    @Override
    public void listFile(ListFileRequest request, StreamObserver<ListFileResponse> responseObserver) {
        UUID userId = getUserId(request.getUserName());
        try {
            ResultSet resultSet = cassandraSession.execute("SELECT file_name FROM files WHERE user_id = ? ALLOW FILTERING",
                    userId);

            for (Row row : resultSet) {
                ListFileResponse response = ListFileResponse.newBuilder()
                        .setFileName(row.getString("file_name"))
                        .build();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
        } catch (Exception err) {
            logger.error("Error: list file: {}", err.getMessage());
        }
    }

    @Override
    public void downloadFile(DownloadFileRequest request, StreamObserver<DownloadFileResponse> responseObserver) {
        String userName = request.getUsername();
        String fileName = request.getFileName();
        UUID userId = getUserId(request.getUsername());

        if (userId == null) {
            logger.error("User not found");
            return;
        }

        Row fileRow = cassandraSession.execute(
                "SELECT file_id, file_path_server, file_size FROM files WHERE user_id = ? AND file_name = ? ALLOW FILTERING",
                userId, fileName).one();


        if (fileRow == null) {
            logger.error("File not found ");
            return;
        }

        UUID fileId = fileRow.getUuid("file_id");
        String filePath = fileRow.getString("file_path_server");
        long fileZise = fileRow.getLong("file_size");
        logger.info("File Data: {} {} {}", fileId, filePath, fileZise);

        File file = new File(filePath);
        if (!file.exists() || file.length() != fileZise) {
            logger.error("Error: file missing or corruted");
            return;
        }

        long totalChunks = (fileZise + chunk_size - 1) / chunk_size;
        long chunkNumber = 0;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            byte[] buffer = new byte[chunk_size];
            int bytesRead;

            while ((bytesRead = raf.read(buffer)) != -1) {
                DownloadFileResponse response = DownloadFileResponse.newBuilder()
                        .setData(ByteString.copyFrom(buffer, 0, bytesRead))
                        .setChunkNumber(chunkNumber)
                        .setTotalChunks(totalChunks)
                        .build();

                responseObserver.onNext(response);
                chunkNumber++;
            }

            responseObserver.onCompleted();
            registerUserAction(userId, fileId, "Download");


        } catch (Exception err) {
            logger.error("Error reading file");
        }
    }

    @Override
    public void deleteFile(DeleteFileRequest request, StreamObserver<DeleteFileResponse> responseObserver) {
        String sessionToken = request.getSessionToken();
        String fileName = request.getFileName();

        String userName = getUsernameFromToken(sessionToken);
        if (userName == null) {
            responseObserver.onNext(DeleteFileResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Invalid or expired sessionToken")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID userId = getUserId(userName);
        if (userId == null) {
            responseObserver.onNext(DeleteFileResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("User not found")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        //see if it belongs to the user
        Row fileRow = getFileRow(userId, fileName);
        if (fileRow == null) {
            responseObserver.onNext(DeleteFileResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("File not found or access denied")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID fileId = fileRow.getUuid("file_id");
        String filePath = fileRow.getString("file_path_server");

        try {
            Path path = Paths.get(filePath);
            Files.deleteIfExists(path);
        } catch (Exception err) {
            responseObserver.onNext(DeleteFileResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to delete file: " + err.getMessage())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        cassandraSession.execute(
                "DELETE FROM files WHERE file_id = ?", fileRow.getUuid("file_id"));

        registerUserAction(userId, fileId, "Delete");

        responseObserver.onNext(DeleteFileResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Filete deleted successfully")
                .build());
        responseObserver.onCompleted();
    }

    private String getUsernameFromToken(String sessionToken) {
        ResultSet rs = cassandraSession.execute("SELECT username FROM session_tokens WHERE \"token\" = ?", sessionToken);
        Row row = rs.one();
        return (row != null) ? row.getString("username") : null;
    }

    private Row getFileRow(UUID userId, String fileName) {
        ResultSet rs = cassandraSession.execute(
                "SELECT file_id, file_path_server FROM files WHERE user_id = ? AND file_name = ? ALLOW FILTERING",
                userId, fileName);
        return rs.one();
    }

    private UUID getUserId(String username) {
        var row = cassandraSession.execute("SELECT user_id FROM users WHERE username = ? ALLOW FILTERING", username).one();
        return row != null ? row.getUuid("user_id") : null;
    }

    private void registerUserAction(UUID userId, UUID fileId, String actionType) {
        UUID actionId = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();

        cassandraSession.execute("INSERT INTO user_actions (action_id, user_id, action_type, file_id, action_timestamp, action_details) " +
                        "VALUES (?, ?, ?, ?, ?, ?)",
                actionId, userId, actionType, fileId, timestamp, "The file.");
    }
}
