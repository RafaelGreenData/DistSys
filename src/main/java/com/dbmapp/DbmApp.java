package com.dbmapp;

import com.dbmapp.server.DatabaseManagementServer;
import com.dbmapp.server.TableManagementServer;

import com.dbmapp.client.DatabaseManagementClient;
// import com.dbmapp.client.TableManagementClient; // Uncomment when implemented

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.swing.SwingUtilities;

public class DbmApp {
    public static void main(String[] args) {
        int port = 50051;

        try {
            // Start gRPC server with all service implementations
            Server server = ServerBuilder.forPort(port)
                    .addService(new DatabaseManagementServer()) // ✅ Database service
                    .addService(new TableManagementServer())     // ✅ Table service (implement next)
                    // .addService(new CSVImportServer())
                    // .addService(new FilteringSortingServer())
                    .build()
                    .start();

            System.out.println("✅ DBMApp Server started on port " + port);
            System.out.println("Press CTRL+C to stop...");

            // Create shared gRPC client channel
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                    .usePlaintext()
                    .build();

            // Initialize clients
            DatabaseManagementClient dbClient = new DatabaseManagementClient(channel);
            // TableManagementClient tableClient = new TableManagementClient(channel); // ✅ Add when ready

            // Option 1: Run CLI or test
            dbClient.createSchema("dev_schema");
            dbClient.listSchemas();
            dbClient.deleteSchema("dev_schema");

            // Option 2: Run GUI
            // SwingUtilities.invokeLater(() -> new DbmAppGUI(dbClient, tableClient));

            // Keep server alive
            server.awaitTermination();

        } catch (Exception e) {
            System.err.println("❌ Failed to start the server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
