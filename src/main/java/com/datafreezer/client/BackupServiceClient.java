package com.datafreezer.client;

import com.datafreezer.common.PropertiesManager;
import com.proto.backupservice.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupServiceClient {

    private static final Logger logger = LoggerFactory.getLogger(BackupServiceClient.class);
    private static final String clienthost = PropertiesManager.getInstance().getProperty("main.host");
    private static final int clientPort = PropertiesManager.getInstance().getIntProperty("grpc.port");
    //private static final int chunk_size = PropertiesManager.getInstance().getIntProperty("chunk.size");
    private static final int chunk_size = 1024 * 1024;

    private final ManagedChannel channel;
    private final BackupServiceGrpc.BackupServiceBlockingStub blockingStub;
    private final BackupServiceGrpc.BackupServiceStub asyncStub;
    private String sessionToken;
    private String currentUser;

    public BackupServiceClient() {
        this.channel = ManagedChannelBuilder.forAddress(clienthost, clientPort)
                .usePlaintext()
                .build();
        this.blockingStub = BackupServiceGrpc.newBlockingStub(channel);
        this.asyncStub = BackupServiceGrpc.newStub(channel);
    }

    public static void main(String[] args) {
        BackupServiceClient client = new BackupServiceClient();
        client.run();
    }

    private void run() {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        System.out.println("DataFreezer - Type 'help' for commands");

        while (running) {
            System.out.print("Alvrz> ");
            String input = scanner.nextLine().trim();
            String[] parts = input.split(" ", 2);

            switch (parts[0]) {
                case "connect":
                    if (parts.length == 2) {
                        String[] credentials = parts[1].split(" ");
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

                case "listFiles":
                    handleListFiles();
                    break;
                case "download":
                    if (parts.length == 2) {
                        handleDownload(parts[1]);
                    } else {
                        System.out.println("Usage: download <file_id>");
                    }
                    break;
                case "delete":
                    if (parts.length == 2) {
                        handleDelete(parts[1]);
                    } else {
                        System.out.println("Usage: delete <file_name>");
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

    private void handleLogin(String username, String password) {
        try {
            LoginResponse response = blockingStub.login(LoginRequest.newBuilder()
                    .setUsername(username)
                    .setPassword(password)
                    .build());

            if (response.getSuccess()) {
                currentUser = username;
                sessionToken = response.getSessionToken();
                System.out.println("Connected successfully!");
            } else {
                System.out.println("Login failed: " + response.getMessage());
            }
        } catch (Exception e) {
            System.out.println("Error connecting: " + e.getMessage());
        }
    }

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

            // Esperar a que termine la subida
            int timeoutSeconds = 30;
            while (!uploadComplete[0] && timeoutSeconds > 0) {
                Thread.sleep(1000);
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

    private void handleListFiles() {
        if (currentUser == null) {
            System.out.println("Please connect first");
            return;
        }

        System.out.println("Fetching file list ...");

        try {
            // Llamada al servicio ListFiles
            Iterator<ListFileResponse> response = blockingStub.listFile(
                    ListFileRequest.newBuilder()
                            .setUserName(currentUser)
                            .build()
            );

            boolean hasFiles = false;
            while (response.hasNext()) {
                System.out.println(response.next().getFileName());
                hasFiles = true;
            }

            if (!hasFiles) {
                logger.warn("No files found for this user");
            }
        } catch (Exception err) {
            logger.error("Error: fetching file list {}", err.getMessage());
        }
    }

    private void handleDownload(String fileName) {
        if (currentUser == null) {
            System.out.println("Please connect first");
            return;
        }

        try {
            DownloadFileRequest request = DownloadFileRequest.newBuilder()
                    .setUsername(currentUser)
                    .setFileName(fileName)
                    .build();

            String downloadsFolderPath = System.getProperty("user.home") + File.separator + "Downloads";
            File downloadsFolder = new File(downloadsFolderPath);

            File localFile = new File(downloadsFolder, fileName);

            int counter = 1;
            while (localFile.exists()) {
                String newFileName = getFileNameWithoutExtension(fileName)
                        + "(" + counter + ")"
                        + getFIleExtention(fileName);
                localFile = new File(downloadsFolder, newFileName);
                counter++;
            }

            try (RandomAccessFile raf = new RandomAccessFile(localFile, "rw")) {
                Iterator<DownloadFileResponse> responseIterator = blockingStub.downloadFile(request);

                while (responseIterator.hasNext()) {
                    DownloadFileResponse response = responseIterator.next();

                    raf.seek(response.getChunkNumber() * chunk_size);
                    raf.write(response.getData().toByteArray());

                    printProgress(response.getChunkNumber() + 1, response.getTotalChunks());
                }
                System.out.println("\nDownload complete: " + fileName);
            }
        } catch (Exception err) {
            logger.error("error: downloading file: {}", err.getMessage());
        }
    }

    private void handleDelete(String fileName) {
        if (currentUser == null) {
            System.out.println("Please connect first");
            return;
        }

        try {
            DeleteFileRequest request = DeleteFileRequest.newBuilder()
                    .setSessionToken(sessionToken)
                    .setFileName(fileName)
                    .build();

            DeleteFileResponse response = blockingStub.deleteFile(request);

            if (response.getSuccess()) {
                System.out.println("File deleted succesfully: " + fileName);
            } else {
                logger.error("Failed to delete file: {}", response.getMessage());
            }
        } catch (Exception err) {
            logger.error("Error while deleting file: {} " + err.getMessage());
        }

    }

    private String getFileNameWithoutExtension(String fileName) {
        int pos = fileName.lastIndexOf(".");
        if (pos > 0) {
            return fileName.substring(0, pos);
        }
        return fileName;
    }

    private String getFIleExtention(String fileName) {
        int pos = fileName.lastIndexOf(".");
        if (pos > 0) {
            return fileName.substring(pos);
        }
        return "";
    }

    private void printProgress(long current, long total) {
        int percentage = (int) (current * 100 / total);
        System.out.print("\rUploading: " + percentage + "%");
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  connect <username> <password> - Connect to server");
        System.out.println("  upload <file_path> - Upload a file");
        System.out.println("  listFiles - List files");
        System.out.println("  Download <file_path> - Download a file");
        System.out.println("  Delete <file_name> - Delete a file");
        System.out.println("  help - Show this help");
        System.out.println("  exit - Exit application");
    }

    private void shutdown() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            logger.info("Client closed");
        } catch (InterruptedException err) {
            logger.error("Error during shutdown: {}", err.getMessage());
        }
    }
} 