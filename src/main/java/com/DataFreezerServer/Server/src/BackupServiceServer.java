package com.DataFreezerServer.Server.src;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

public class BackupServiceServer {
    public static void main(String[] args) throws InterruptedException, IOException {
        Server server = ServerBuilder.forPort(50051)
                .addService(new BackupServiceServerImpl())
                .build();

        //Server Start
        server.start();
        System.out.println("Server Start");

        server.awaitTermination();
    }
}
