package com.DataFreezerServer.Server.src;

import com.common.PropertiesManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupServiceServer {

    /**
     * Logger for the BackupServiceServer class.
     */
    private static final Logger logger = LoggerFactory.getLogger(BackupServiceServer.class);
    private static final int portGrpc = PropertiesManager.getInstance().getIntProperty("grpc.port");

    /**
     * The main entry point to start the gRPC server and add the backup service.
     *
     * @param args Command-line arguments.
     * @throws InterruptedException If the thread is interrupted while waiting.
     * @throws IOException          If an error occurs during server startup.
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        Server server = ServerBuilder.forPort(portGrpc)
                .addService(new BackupServiceServerImpl())
                .build();

        //Server Start
        server.start();
        System.out.println("Server Start ...");
        logger.info("Server Start ...");
        server.awaitTermination();
    }
}
