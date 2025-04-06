package com.dbmapp.client;

import com.dbmapp.grpc.TableManagementGrpc;
import com.dbmapp.grpc.SchemaRequest;
import com.dbmapp.grpc.TableRequest;
import com.dbmapp.grpc.TableListResponse;
import com.dbmapp.grpc.TableDataResponse;
import com.dbmapp.grpc.RowData;
import com.dbmapp.grpc.ConfirmationMessage;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.Scanner;

// gRPC client for interacting with TableManagement service
public class TableManagementClient {

    // Blocking stub to make synchronous calls
    private final TableManagementGrpc.TableManagementBlockingStub blockingStub;

    // Constructor: creates stub from provided channel
    public TableManagementClient(ManagedChannel channel) {
        this.blockingStub = TableManagementGrpc.newBlockingStub(channel);
    }

    // List all tables in a given schema
    public void getTables(String schemaName) {
        SchemaRequest request = SchemaRequest.newBuilder()
                .setSchemaName(schemaName)
                .build();

        try {
            TableListResponse response = blockingStub.getTables(request);
            List<String> tables = response.getTablesList();

            System.out.println("📦 Tables in schema '" + schemaName + "':");
            for (String table : tables) {
                System.out.println(" - " + table);
            }

        } catch (StatusRuntimeException e) {
            System.err.println("❌ Failed to get tables: " + e.getMessage());
        }
    }

    // View the first N rows from a given table in a schema
    public void viewTableData(String schemaName, String tableName, int rowLimit) {
        TableRequest request = TableRequest.newBuilder()
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setRowLimit(rowLimit)
                .build();

        try {
            TableDataResponse response = blockingStub.viewTableData(request);

            List<String> columns = response.getColumnNamesList();
            List<RowData> rows = response.getRowsList();

            System.out.println("\n📊 Data from table '" + tableName + "' (limit " + rowLimit + " rows):");
            System.out.println(String.join(" | ", columns));

            for (RowData row : rows) {
                System.out.println(String.join(" | ", row.getValuesList()));
            }

        } catch (StatusRuntimeException e) {
            System.err.println("❌ Failed to view table data: " + e.getMessage());
        }
    }

    // Drop a table from a schema
    public void dropTable(String schemaName, String tableName) {
        TableRequest request = TableRequest.newBuilder()
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .build();

        try {
            ConfirmationMessage response = blockingStub.dropTable(request);
            System.out.println((response.getOk() ? "✅" : "❌") + " " + response.getMessage());

        } catch (StatusRuntimeException e) {
            System.err.println("❌ Failed to drop table: " + e.getMessage());
        }
    }

    // Optional CLI interface for testing
    public static void main(String[] args) {
        // This is just for testing purposes; the real entry point is DbmApp
        System.out.println("This is a gRPC client and should be called from DbmApp.");
    }
}
