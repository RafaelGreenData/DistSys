package com.dbmapp.client;

import com.dbmapp.grpc.TableManagementGrpc;
import com.dbmapp.grpc.TableManagementOuterClass.SchemaRequest;
import com.dbmapp.grpc.TableManagementOuterClass.TableRequest;
import com.dbmapp.grpc.TableManagementOuterClass.TableListResponse;
import com.dbmapp.grpc.TableManagementOuterClass.TableDataResponse;
import com.dbmapp.grpc.TableManagementOuterClass.RowData;
import com.dbmapp.grpc.TableManagementOuterClass.ConfirmationMessage;
import com.dbmapp.grpc.TableManagementOuterClass.SQLRequest;
import com.dbmapp.grpc.TableManagementOuterClass.GenericResponse;


import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.util.List;

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
    
    public void executeSQL(String schema, String sqlStatement) {
        SQLRequest request = SQLRequest.newBuilder()
                .setSchemaName(schema)
                .setSqlStatement(sqlStatement)
                .build();
        GenericResponse response = blockingStub.executeSQL(request);

        if (response.getOk()) {
            System.out.println("✅ Table created successfully.");
        } else {
            System.out.println("❌ Failed to create table: " + response.getMessage());
        }

    /**
    // Optional CLI interface for testing
    public static void main(String[] args) {
        //testing
        System.out.println("This is a gRPC client and should be called from DbmApp.");
    }
    */
}
}

