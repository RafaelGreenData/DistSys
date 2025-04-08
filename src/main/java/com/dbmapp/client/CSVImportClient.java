package com.dbmapp.client;

import com.dbmapp.grpc.CSVImportGrpc;
import com.dbmapp.grpc.CSVImportOuterClass.CSVImportRequest;
import com.dbmapp.grpc.CSVImportOuterClass.ConfirmationMessage;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * Client for importing CSV data into a remote table using gRPC.
 */
public class CSVImportClient {
    private final CSVImportGrpc.CSVImportBlockingStub blockingStub;
    private final CSVImportGrpc.CSVImportStub asyncStub;
    private final TableManagementClient tableClient;

    public CSVImportClient(ManagedChannel channel, TableManagementClient tableClient) {
        this.blockingStub = CSVImportGrpc.newBlockingStub(channel);
        this.asyncStub = CSVImportGrpc.newStub(channel);
        this.tableClient = tableClient;
    }

    /**
     * Generic CSV import method compatible with any structure
     */
    public void importCSVAndCreateTable(String schema, String tableName, String filePath) {
        try (Reader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            // Sanitize headers for SQL table creation
            List<String> originalHeaders = csvParser.getHeaderNames();
            List<String> sqlHeaders = originalHeaders.stream()
                    .map(h -> h.trim().replaceAll("[^a-zA-Z0-9_]", "_"))
                    .collect(Collectors.toList());

            // Step 1: Create table
            String createSQL = "CREATE TABLE IF NOT EXISTS " + schema + "." + tableName + " ("
                    + sqlHeaders.stream()
                        .map(h -> "`" + h + "` VARCHAR(255)")
                        .collect(Collectors.joining(", "))
                    + ")";
            try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + schema, "root", "00bob456");
                 Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createSQL);
                System.out.println("✅ Table created successfully.");
            }

            // Step 2: Send rows to gRPC server
            CountDownLatch latch = new CountDownLatch(1);
            StreamObserver<ConfirmationMessage> responseObserver = new StreamObserver<ConfirmationMessage>() {
                @Override public void onNext(ConfirmationMessage msg) {
                    System.out.println((msg.getOk() ? "✅" : "❌") + " " + msg.getMessage());
                }

                @Override public void onError(Throwable t) {
                    System.err.println("❌ Stream error: " + t.getMessage());
                    latch.countDown();
                }

                @Override public void onCompleted() {
                    System.out.println("✅ CSV import completed.");
                    latch.countDown();
                }
            };

            StreamObserver<CSVImportRequest> requestObserver = asyncStub.importCSV(responseObserver);
            int line = 1;

            for (CSVRecord record : csvParser) {
                // Extract values by index (not sanitized name)
                List<String> values = originalHeaders.stream()
                        .map(h -> record.get(h).trim())
                        .collect(Collectors.toList());

                CSVImportRequest request = CSVImportRequest.newBuilder()
                        .setSchemaName(schema)
                        .setTableName(tableName)
                        .addAllValues(values)
                        .setLineNumber(line++)
                        .build();

                requestObserver.onNext(request);
            }

            requestObserver.onCompleted();
            latch.await();

        } catch (Exception e) {
            System.err.println("❌ Error importing CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
