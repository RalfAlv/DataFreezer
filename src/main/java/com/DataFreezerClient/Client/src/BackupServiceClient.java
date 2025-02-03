
package com.DataFreezerClient.Client.src;

import com.proto.backupservice.BackupServiceGrpc;
import com.proto.backupservice.LoginRequest;
import com.proto.backupservice.LoginResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupServiceClient {

    private static final Logger logger = LoggerFactory.getLogger(BackupServiceClient.class);

    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        // Stub for calling server methods
        BackupServiceGrpc.BackupServiceBlockingStub stub = BackupServiceGrpc.newBlockingStub(channel);

        Scanner scanner = new Scanner(System.in);

        label:
        while (true) {
            System.out.print("Alv> ");
            String input = scanner.nextLine().trim();
            logger.debug("Received command: {}", input);

            if (input.startsWith("connect ")) {
                // Split the entry into parts
                String[] parts = input.split(" ");
                if (parts.length == 3) {
                    String username = parts[1];
                    String password = parts[2];

                    // Create the login request
                    LoginRequest loginRequest = LoginRequest.newBuilder()
                            .setUsername(username)
                            .setPassword(password)
                            .build();

                    //Call the login method on the server
                    LoginResponse loginResponse = stub.login(loginRequest);

                    // Show the answer
                    if (loginResponse.getSuccess()) {
                        System.out.println("Login Successful. Session Token: " + loginResponse.getSessionToken());
                    } else {
                        System.out.println("Error: Login failed." + loginResponse.getMessage());
                    }
                } else {
                    System.out.println("Incorrect format. Use: connect <user> <password>");
                }
            } else if (input.equals("exit")) {
                System.out.println("Exiting the application...");
                break label;
            } else if (input.equals("help")) {
                System.out.println("Available commands:");
                System.out.println(" connect <user> <password> - Connect to the server");
                System.out.println(" exit - Exit the application");
                System.out.println(" help - Show this help");
            } else {
                System.out.println("Unrecognized command");
            }
        }

        // Close the channel
        channel.shutdown();
        System.out.println("Client closed");
    }
}
