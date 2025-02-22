package com.datafreezer.server;

import com.datafreezer.common.PropertiesManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for starting the gRPC server for the backup service.
 * <p>The class sets up the server to listen on port 50051 and adds the backup service.</p>
 * <p>The server handles client connections and waits for the server to shut down indefinitely.</p>
 */

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
