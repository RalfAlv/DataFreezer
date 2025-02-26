package com.DataFreezerClient.Client.src;

import com.common.PropertiesManager;
import com.proto.backupservice.BackupServiceGrpc;
import com.proto.backupservice.LoginRequest;
import com.proto.backupservice.LoginResponse;
import com.proto.backupservice.UploadFileResponse;
import com.proto.backupservice.UploadFileRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupServiceClient {

    private static final Logger logger = LoggerFactory.getLogger(BackupServiceClient.class);
    private static final String clienthost = PropertiesManager.getInstance().getProperty("main.host");
    private static final int clientPort = PropertiesManager.getInstance().getIntProperty("grpc.port");
    private static final int chunk_size = Integer.parseInt(PropertiesManager.getInstance().getProperty("chunk.size"));

    private final ManagedChannel channel;
    private final BackupServiceGrpc.BackupServiceBlockingStub blockingStub;
    private final BackupServiceGrpc.BackupServiceStub asyncStub;
    private String currentUser;

    /**
     * Initializes the gRPC client with blocking and stubs.
     */
    public BackupServiceClient() {
        this.channel = ManagedChannelBuilder.forAddress(clienthost, clientPort)
                .usePlaintext()
                .build();
        this.blockingStub = BackupServiceGrpc.newBlockingStub(channel);
        this.asyncStub = BackupServiceGrpc.newStub(channel);
    }

    /**
     * Initializes and runs the client.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        BackupServiceClient client = new BackupServiceClient();
        client.run();
    }

    /**
     * Starts the interactive CLI, processing user commands.
     */
    private void run() {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        System.out.println("DataFreezer - Type 'help' for commands");

        while (running) {
            System.out.print("Alvrz> ");
            String input = scanner.nextLine().trim();
            String[] parts = input.trim().split("\\s+", 2);

            switch (parts[0]) {
                case "connect":
                    if (parts.length == 2) {
                        String[] credentials = parts[1].trim().split("\\s+");
                        if (credentials.length == 2) {
                            handleLogin(credentials[0], credentials[1]);
                        } else {
                            System.out.println("Usage: connect <username> <password>");
                        }
                    }
                    break;

                case "upload":
                    if (parts.length == 2) {
                        handleUpload(parts[1]);
                    } else {
                        System.out.println("Usage: upload <filepath>");
                    }
                    break;

                case "exit":
                    running = false;
                    break;

                case "help":
                    printHelp();
                    break;

                default:
                    System.out.println("Unknown command. Type 'help' for available commands.");
            }
        }

        shutdown();
    }

    /**
     * Sends a login request and sets the current user if successful.
     *
     * @param username the username of the user attempting to log in
     * @param password the password of the user attempting to log in
     */
    private void handleLogin(String username, String password) {
        try {
            LoginResponse response = blockingStub.login(LoginRequest.newBuilder()
                    .setUsername(username)
                    .setPassword(password)
                    .build());

            if (response.getSuccess()) {
                currentUser = username;
                System.out.println("Connected successfully!");
            } else {
                System.out.println("Login failed: " + response.getMessage());
            }
        } catch (Exception e) {
            System.out.println("Error connecting: " + e.getMessage());
        }
    }

    /**
     * Handles file upload to the server.
     * Ensures the user is connected and the file exists before uploading.
     *
     * @param filePath path of the file to upload
     */
    private void handleUpload(String filePath) {
        if (currentUser == null) {
            System.out.println("Please connect first");
            return;
        }

        File file = new File(filePath);
        if (!file.exists()) {
            logger.error("File not found: {}", filePath);
            return;
        }

        final boolean[] uploadComplete = {false};

        StreamObserver<UploadFileResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(UploadFileResponse response) {
                if (response.getSucess()) {
                    logger.info("\nFile uploaded successfully! File ID: {}", response.getFileId());
                } else {
                    logger.error("\nUpload failed: {}", response.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.error("Upload error: {}", t.getMessage());
                uploadComplete[0] = true;
            }

            @Override
            public void onCompleted() {
                uploadComplete[0] = true;
            }
        };

        StreamObserver<UploadFileRequest> requestObserver = asyncStub.uploadFile(responseObserver);

        try {
            long fileSize = file.length();
            long totalChunks = (fileSize + chunk_size - 1) / chunk_size;

            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                byte[] buffer = new byte[chunk_size];
                long chunkNumber = 0;
                int bytesRead;

                while ((bytesRead = raf.read(buffer)) != -1) {
                    UploadFileRequest request = UploadFileRequest.newBuilder()
                            .setUsername(currentUser)
                            .setFileName(file.getName())
                            .setData(com.google.protobuf.ByteString.copyFrom(buffer, 0, bytesRead))
                            .setChunkNumber(chunkNumber)
                            .setTotalChunks(totalChunks)
                            .build();

                    requestObserver.onNext(request);
                    printProgress(chunkNumber + 1, totalChunks);
                    chunkNumber++;
                }
            }

            requestObserver.onCompleted();

            // Wait for the upload to finish
            int timeoutSeconds = 30;
            while (!uploadComplete[0] && timeoutSeconds > 0) {
                //lo vamos a remplazar sin tiempo por los archivos grandes
                TimeUnit.MILLISECONDS.sleep(1000);
                timeoutSeconds--;
            }

            if (!uploadComplete[0]) {
                logger.error("Upload timed out for file: {}", filePath);
            }

        } catch (Exception e) {
            logger.error("\nError uploading file: {}", e.getMessage());
            requestObserver.onError(e);
        }
    }

    /**
     * Displays the upload progress as a percentage.
     *
     * @param current bytes uploaded so far
     * @param total   total bytes to upload
     */
    private void printProgress(long current, long total) {
        int percentage = (int) (current * 100 / total);
        System.out.print("\rUploading: " + percentage + "%");
    }

    /**
     * List of available commands.
     */
    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  connect <username> <password> - Connect to server");
        System.out.println("  upload <file_path> - Upload a file");
        System.out.println("  help - Show this help");
        System.out.println("  exit - Exit application");
    }

    /**
     * Shut down the gRPC channel.
     */
    private void shutdown() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            logger.info("Client closed");
        } catch (InterruptedException err) {
            logger.error("Error during shutdown: {}", err.getMessage());
        }
    }
} 