package com.dbmapp.client;

import com.dbmapp.grpc.DatabaseManagementGrpc;
import com.dbmapp.grpc.DatabaseManagementGrpc.DatabaseManagementBlockingStub;
import com.dbmapp.grpc.DatabaseManagementServiceImpl.SchemaRequest;
import com.dbmapp.grpc.DatabaseManagementServiceImpl.SchemaResponse;
import com.dbmapp.grpc.DatabaseManagementServiceImpl.ConfirmationMessage;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DatabaseManagementClient {

    public static void main(String[] args) {
        // Connect to server
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        // Create stub
        DatabaseManagementBlockingStub stub = DatabaseManagementGrpc.newBlockingStub(channel);

        // Example schema name
        String schemaName = "test_schema";

        // 1. Create schema
        SchemaRequest createRequest = SchemaRequest.newBuilder()
                .setSchemaName(schemaName)
                .build();

        try {
            ConfirmationMessage createResponse = stub.createSchema(createRequest);
            System.out.println("Create Schema Response: " + createResponse.getMessage());
        } catch (Exception e) {
            System.out.println("Error creating schema: " + e.getMessage());
        }

        // 2. List schemas
        try {
            System.out.println("\nListing Schemas:");
            stub.listSchemas(Empty.newBuilder().build()).forEachRemaining(response -> {
                System.out.println("- " + response.getSchemaName());
            });
        } catch (Exception e) {
            System.out.println("Error listing schemas: " + e.getMessage());
        }

        // 3. Delete schema
        try {
            ConfirmationMessage deleteResponse = stub.deleteSchema(createRequest);
            System.out.println("\nDelete Schema Response: " + deleteResponse.getMessage());
        } catch (Exception e) {
            System.out.println("Error deleting schema: " + e.getMessage());
        }

        // Shutdown
        channel.shutdown();
    }
}
