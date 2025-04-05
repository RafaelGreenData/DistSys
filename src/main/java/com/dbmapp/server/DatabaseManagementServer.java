package com.dbmapp.server;

import com.dbmapp.grpc.DatabaseManagementGrpc;
import com.dbmapp.grpc.DatabaseManagementServiceImpl.SchemaRequest;
import com.dbmapp.grpc.DatabaseManagementServiceImpl.SchemaResponse;
import com.dbmapp.grpc.DatabaseManagementServiceImpl.ConfirmationMessage;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.sql.*;


/**
 * This class implements the DatabaseManagement gRPC service.
 * It handles schema (database) creation, listing, and deletion.
 */
public class DatabaseManagementServer extends DatabaseManagementGrpc.DatabaseManagementImplBase {

    // Replace with your MySQL credentials
    private static final String DB_URL = "jdbc:mysql://localhost:3306/";
    private static final String USER = "root";
    private static final String PASSWORD = "00bob456";

    @Override
    public void createSchema(SchemaRequest request, StreamObserver<ConfirmationMessage> responseObserver) {
        String schemaName = request.getSchemaName();

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE DATABASE " + schemaName);

            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setOk(true)
                    .setMessage("Schema '" + schemaName + "' created successfully.")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SQLException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription("Failed to create schema: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void listSchemas(Empty request, StreamObserver<SchemaResponse> responseObserver) {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {

            while (rs.next()) {
                SchemaResponse response = SchemaResponse.newBuilder()
                        .setSchemaName(rs.getString(1))
                        .build();
                responseObserver.onNext(response);
            }

            responseObserver.onCompleted();

        } catch (SQLException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Error listing schemas: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void deleteSchema(SchemaRequest request, StreamObserver<ConfirmationMessage> responseObserver) {
        String schemaName = request.getSchemaName();

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("DROP DATABASE " + schemaName);

            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setOk(true)
                    .setMessage("Schema '" + schemaName + "' deleted successfully.")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SQLException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to delete schema: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
