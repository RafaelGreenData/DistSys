package com.dbmapp.client;

import com.dbmapp.grpc.CSVImportGrpc;
import com.dbmapp.grpc.CSVImportGrpc.CSVImportStub;
import com.dbmapp.grpc.CSVImportOuterClass.CSVImportRequest;
import com.dbmapp.grpc.CSVImportOuterClass.ConfirmationMessage;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Client class to handle CSV import using gRPC client-streaming.
 */
public class CSVImportClient {
    private final CSVImportStub asyncStub;
    private final TableManagementClient tableClient;

    public CSVImportClient(ManagedChannel channel, TableManagementClient tableClient) {
        this.asyncStub = CSVImportGrpc.newStub(channel);
        this.tableClient = tableClient;
    }

    public void importCSVAndCreateTable(String schema, String tableName, String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                System.err.println("❌ CSV file is empty or missing header row.");
                return;
            }

            String[] headers = headerLine.split(",");

            StringBuilder createSQL = new StringBuilder("CREATE TABLE ")
                    .append(schema).append(".").append(tableName).append(" (");

            for (int i = 0; i < headers.length; i++) {
                createSQL.append("`").append(headers[i].trim()).append("` VARCHAR(255)");
                if (i < headers.length - 1) {
                    createSQL.append(", ");
                }
            }
            createSQL.append(")");

            tableClient.executeSQL(schema, createSQL.toString());

            CountDownLatch latch = new CountDownLatch(1);

            StreamObserver<ConfirmationMessage> responseObserver = new StreamObserver<ConfirmationMessage>() {
                @Override
                public void onNext(ConfirmationMessage value) {
                    System.out.println((value.getOk() ? "✅ Success: " : "❌ Error: ") + value.getMessage());
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("❌ Stream error: " + t.getMessage());
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    System.out.println("📥 CSV import completed.");
                    latch.countDown();
                }
            };

            StreamObserver<CSVImportRequest> requestObserver = asyncStub.importCSV(responseObserver);

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                CSVImportRequest.Builder request = CSVImportRequest.newBuilder()
                        .setSchemaName(schema)
                        .setTableName(tableName);

                for (String value : values) {
                    request.addValues(value.trim());
                }

                requestObserver.onNext(request.build());
            }

            requestObserver.onCompleted();
            latch.await();

        } catch (IOException | InterruptedException e) {
            System.err.println("❌ Failed to import CSV: " + e.getMessage());
        }
    }
}
