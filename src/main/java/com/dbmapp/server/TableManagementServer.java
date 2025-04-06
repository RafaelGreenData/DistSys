package com.dbmapp.server;

// Correct imports for gRPC base class and generated messages
import com.dbmapp.grpc.TableManagementGrpc;
import com.dbmapp.grpc.SchemaRequest;
import com.dbmapp.grpc.TableRequest;
import com.dbmapp.grpc.TableListResponse;
import com.dbmapp.grpc.TableDataResponse;
import com.dbmapp.grpc.RowData;
import com.dbmapp.grpc.ConfirmationMessage;

import io.grpc.stub.StreamObserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

// gRPC server implementation for the TableManagement service
public class TableManagementServer extends TableManagementGrpc.TableManagementImplBase {

    // Database credentials and JDBC URL prefix
    private static final String JDBC_URL_PREFIX = "jdbc:mysql://localhost:3306/";
    private static final String DB_USER = "root";  
    private static final String DB_PASSWORD = "00bob456"; 

    // Retrieve a list of tables from the given schema
    @Override
    public void getTables(SchemaRequest request, StreamObserver<TableListResponse> responseObserver) {
        String schemaName = request.getSchemaName();
        TableListResponse.Builder responseBuilder = TableListResponse.newBuilder();

        try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + schemaName, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES")) {

            // Add all table names to the response
            while (rs.next()) {
                responseBuilder.addTables(rs.getString(1));
            }

            // Send response
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (SQLException e) {
            // Log error and send empty result to client
            e.printStackTrace();
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }

    // View the first N rows of a table
    @Override
    public void viewTableData(TableRequest request, StreamObserver<TableDataResponse> responseObserver) {
        String schemaName = request.getSchemaName();
        String tableName = request.getTableName();
        int rowLimit = request.getRowLimit(); // 🆕 User-defined limit

        try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + schemaName, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT " + rowLimit)) {

            // Get column metadata
            int columnCount = rs.getMetaData().getColumnCount();
            TableDataResponse.Builder responseBuilder = TableDataResponse.newBuilder();

            // Add column names
            for (int i = 1; i <= columnCount; i++) {
                responseBuilder.addColumnNames(rs.getMetaData().getColumnName(i));
            }

            // Add row data
            while (rs.next()) {
                RowData.Builder rowBuilder = RowData.newBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    rowBuilder.addValues(rs.getString(i));
                }
                responseBuilder.addRows(rowBuilder.build());
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (SQLException e) {
            e.printStackTrace();
            responseObserver.onCompleted();
        }
    }

    // Drop a table from the schema
    @Override
    public void dropTable(TableRequest request, StreamObserver<ConfirmationMessage> responseObserver) {
        String schemaName = request.getSchemaName();
        String tableName = request.getTableName();

        try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + schemaName, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {

            String sql = "DROP TABLE IF EXISTS " + tableName;
            stmt.executeUpdate(sql);

            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setOk(true)
                    .setMessage("Table '" + tableName + "' dropped successfully.")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SQLException e) {
            ConfirmationMessage error = ConfirmationMessage.newBuilder()
                    .setOk(false)
                    .setMessage("Error dropping table: " + e.getMessage())
                    .build();
            responseObserver.onNext(error);
            responseObserver.onCompleted();
        }
    }
}
