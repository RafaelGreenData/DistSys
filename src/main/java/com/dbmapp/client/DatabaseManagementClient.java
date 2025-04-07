package com.dbmapp.client;

import com.dbmapp.grpc.DatabaseManagementGrpc;
import com.dbmapp.grpc.DatabaseManagementOuterClass.ConfirmationMessage;
import com.dbmapp.grpc.DatabaseManagementOuterClass.SchemaRequest;
import com.dbmapp.grpc.DatabaseManagementOuterClass.SchemaResponse;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Iterator;

// Client wrapper for interacting with the DatabaseManagement gRPC service
public class DatabaseManagementClient {

    // gRPC blocking stub
    private final DatabaseManagementGrpc.DatabaseManagementBlockingStub blockingStub;

    // Constructor: initializes stub with a managed channel
    public DatabaseManagementClient(ManagedChannel channel) {
        blockingStub = DatabaseManagementGrpc.newBlockingStub(channel);
    }

    // Create a new schema on the server
    public void createSchema(String schemaName) {
        SchemaRequest request = SchemaRequest.newBuilder()
                .setSchemaName(schemaName)
                .build();

        try {
            ConfirmationMessage response = blockingStub.createSchema(request);
            System.out.println("➕ Create Schema: " + response.getMessage());
        } catch (StatusRuntimeException e) {
            System.err.println("gRPC error during schema creation: " + e.getStatus().getDescription());
        }
    }

    // Delete a schema from the server
    public void deleteSchema(String schemaName) {
        SchemaRequest request = SchemaRequest.newBuilder()
                .setSchemaName(schemaName)
                .build();

        try {
            ConfirmationMessage response = blockingStub.deleteSchema(request);
            System.out.println("❌ Delete Schema: " + response.getMessage());
        } catch (StatusRuntimeException e) {
            System.err.println("gRPC error during schema deletion: " + e.getStatus().getDescription());
        }
    }

    // List all schemas available on the server
    public void listSchemas() {
        try {
            Iterator<SchemaResponse> responses = blockingStub.listSchemas(Empty.getDefaultInstance());
            System.out.println("📂 Schemas in database:");
            while (responses.hasNext()) {
                SchemaResponse schema = responses.next();
                System.out.println(" - " + schema.getSchemaName());
            }
        } catch (StatusRuntimeException e) {
            System.err.println("gRPC error during schema listing: " + e.getStatus().getDescription());
        }
    }
}
