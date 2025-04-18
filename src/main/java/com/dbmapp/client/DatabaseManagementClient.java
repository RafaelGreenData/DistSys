package com.dbmapp.client;

import com.dbmapp.grpc.DatabaseManagementGrpc;
import com.dbmapp.grpc.DatabaseManagementOuterClass.ConfirmationMessage;
import com.dbmapp.grpc.DatabaseManagementOuterClass.SchemaRequest;
import com.dbmapp.grpc.DatabaseManagementOuterClass.SchemaResponse;
import com.google.protobuf.Empty;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;

public class DatabaseManagementClient {

    // gRPC blocking and async stubs
    private final DatabaseManagementGrpc.DatabaseManagementBlockingStub blockingStub;
    private final DatabaseManagementGrpc.DatabaseManagementStub asyncStub;

    // Constructor: initializes both stubs with a managed channel
    public DatabaseManagementClient(ManagedChannel channel) {
        this.blockingStub = DatabaseManagementGrpc.newBlockingStub(channel);
        this.asyncStub = DatabaseManagementGrpc.newStub(channel);
    }

    // Stream schema names asynchronously
    public void listSchemas(StreamObserver<SchemaResponse> observer) {
        asyncStub.listSchemas(Empty.getDefaultInstance(), observer);
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

    // Optional: CLI listing of schemas (used for testing/debug)
   public List<String> listSchemas() {
    List<String> schemaList = new ArrayList<>();

    try {
        Iterator<SchemaResponse> responses = blockingStub.listSchemas(Empty.newBuilder().build());
        while (responses.hasNext()) {
            schemaList.add(responses.next().getSchemaName());
        }
    } catch (Exception e) {
        System.err.println("Error while listing schemas: " + e.getMessage());
    }

    return schemaList;
}

    
    public DatabaseManagementGrpc.DatabaseManagementBlockingStub getBlockingStub() {
        return blockingStub;
    }
}
