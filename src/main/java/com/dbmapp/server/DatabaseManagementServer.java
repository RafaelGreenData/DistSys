package com.dbmapp.server;

import com.dbmapp.grpc.DatabaseManagementGrpc;
import com.dbmapp.grpc.DatabaseManagementServiceImpl.*;

import io.grpc.stub.StreamObserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

// gRPC server implementation for the DatabaseManagement service
public class DatabaseManagementServer extends DatabaseManagementGrpc.DatabaseManagementImplBase {

    // Database credentials and JDBC URL
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306";
    private static final String DB_USER = "root";        // Update with your user
    private static final String DB_PASSWORD = "00bob456"; // Update with your password

    // Create a new database schema
    @Override
    public void createSchema(SchemaRequest request, StreamObserver<ConfirmationMessage> responseObserver) {
        String schemaName = request.getSchemaName();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {

            String sql = "CREATE DATABASE IF NOT EXISTS " + schemaName;
            stmt.executeUpdate(sql);

            // Build and send success response
            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setOk(true)
                    .setMessage("Schema '" + schemaName + "' created successfully.")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SQLException e) {
            // Handle SQL error and send error response
            ConfirmationMessage errorResponse = ConfirmationMessage.newBuilder()
                    .setOk(false)
                    .setMessage("Error creating schema: " + e.getMessage())
                    .build();
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    // Stream all database schemas
    @Override
    public void listSchemas(com.google.protobuf.Empty request, StreamObserver<SchemaResponse> responseObserver) {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {

            String sql = "SHOW DATABASES";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                String schema = rs.getString(1);

                // Stream each schema name back to the client
                SchemaResponse response = SchemaResponse.newBuilder()
                        .setSchemaName(schema)
                        .build();
                responseObserver.onNext(response);
            }

            responseObserver.onCompleted();

        } catch (SQLException e) {
            // Print error to server console for debugging
            e.printStackTrace();
            responseObserver.onCompleted(); // Gracefully close even on error
        }
    }

    // Delete a database schema
    @Override
    public void deleteSchema(SchemaRequest request, StreamObserver<ConfirmationMessage> responseObserver) {
        String schemaName = request.getSchemaName();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {

            String sql = "DROP DATABASE IF EXISTS " + schemaName;
            stmt.executeUpdate(sql);

            // Build and send success response
            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setOk(true)
                    .setMessage("Schema '" + schemaName + "' deleted successfully.")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SQLException e) {
            // Handle SQL error and send error response
            ConfirmationMessage errorResponse = ConfirmationMessage.newBuilder()
                    .setOk(false)
                    .setMessage("Error deleting schema: " + e.getMessage())
                    .build();
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
}
