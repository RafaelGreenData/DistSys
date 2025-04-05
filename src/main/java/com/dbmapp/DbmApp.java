package com.dbmapp;

import com.dbmapp.server.DatabaseManagementServer;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class DbmApp {
    public static void main(String[] args) {
        int port = 50051;

        try {
            Server server = ServerBuilder.forPort(port)
                    .addService(new DatabaseManagementServer())
                    .build()
                    .start();

            System.out.println("✅ DBMApp Server started on port " + port);
            System.out.println("Press CTRL+C to stop...");

            server.awaitTermination();

        } catch (Exception e) {
            System.err.println("❌ Failed to start the server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
