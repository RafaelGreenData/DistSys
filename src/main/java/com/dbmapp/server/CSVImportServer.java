package com.dbmapp.server;

import com.dbmapp.grpc.CSVImportGrpc;
import com.dbmapp.grpc.CSVImportOuterClass.CSVImportRequest;
import com.dbmapp.grpc.CSVImportOuterClass.ConfirmationMessage;
import io.grpc.stub.StreamObserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * Server-side implementation for importing CSV data via gRPC client streaming.
 */
public class CSVImportServer extends CSVImportGrpc.CSVImportImplBase {

    @Override
    public StreamObserver<CSVImportRequest> importCSV(StreamObserver<ConfirmationMessage> responseObserver) {
        return new StreamObserver<CSVImportRequest>() {

            String schema;
            String table;
            int columnCount = -1;
            List<List<String>> allRows = new ArrayList<>();

            @Override
            public void onNext(CSVImportRequest request) {
                // Capture schema and table only once
                if (schema == null) {
                    schema = request.getSchemaName();
                    table = request.getTableName();
                    columnCount = request.getValuesCount();
                }

                // Sanity check
                if (request.getValuesCount() != columnCount) {
                    System.err.println("⚠️ Line " + request.getLineNumber() +
                            ": Expected " + columnCount + " columns, but got " + request.getValuesCount());
                    return;
                }

                allRows.add(request.getValuesList());
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                responseObserver.onNext(ConfirmationMessage.newBuilder()
                        .setOk(false)
                        .setMessage("Server error: " + t.getMessage())
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                try (Connection conn = DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/" + schema, "root", "00bob456")) {

                    if (allRows.isEmpty()) {
                        responseObserver.onNext(ConfirmationMessage.newBuilder()
                                .setOk(false)
                                .setMessage("No data received.")
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }

                    // Prepare the INSERT SQL statement
                    String placeholders = String.join(", ", java.util.Collections.nCopies(columnCount, "?"));
                    String sql = "INSERT INTO " + schema + "." + table + " VALUES (" + placeholders + ")";
                    PreparedStatement stmt = conn.prepareStatement(sql);

                    for (List<String> row : allRows) {
                        for (int i = 0; i < columnCount; i++) {
                            stmt.setString(i + 1, row.get(i));
                        }
                        stmt.addBatch();
                    }

                    stmt.executeBatch();

                    responseObserver.onNext(ConfirmationMessage.newBuilder()
                            .setOk(true)
                            .setMessage("All rows inserted successfully.")
                            .build());

                } catch (Exception e) {
                    e.printStackTrace();
                    responseObserver.onNext(ConfirmationMessage.newBuilder()
                            .setOk(false)
                            .setMessage("Insert failed: " + e.getMessage())
                            .build());
                }

                responseObserver.onCompleted();
            }
        };
    }
}
