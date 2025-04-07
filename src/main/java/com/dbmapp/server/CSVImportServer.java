package com.dbmapp.server;

import com.dbmapp.grpc.CSVImportGrpc;
import com.dbmapp.grpc.CSVImportOuterClass;

import io.grpc.stub.StreamObserver;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

// gRPC server implementation for CSVImport service
public class CSVImportServer extends CSVImportGrpc.CSVImportImplBase {

    private static final String JDBC_URL_PREFIX = "jdbc:mysql://localhost:3306/";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "00bob456";

    // Unary RPC to fetch headers from a CSV file
    @Override
    public void getCSVHeaders(
        CSVImportOuterClass.CSVFileRequest request,
        StreamObserver<CSVImportOuterClass.CSVHeadersResponse> responseObserver
    ) {
        String filePath = request.getFilePath();
        CSVImportOuterClass.CSVHeadersResponse.Builder responseBuilder =
                CSVImportOuterClass.CSVHeadersResponse.newBuilder();

        try (Reader reader = new FileReader(filePath);
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            List<String> headers = parser.getHeaderNames();
            responseBuilder.addAllHeaders(headers);

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            e.printStackTrace();
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }

    // Client streaming RPC to import rows of data
    @Override
    public StreamObserver<CSVImportOuterClass.CSVImportRequest> importCSV(
        StreamObserver<CSVImportOuterClass.ConfirmationMessage> responseObserver
    ) {
        return new StreamObserver<CSVImportOuterClass.CSVImportRequest>() {

            private String schemaName = null;
            private String tableName = null;
            private StringBuilder insertSQL = new StringBuilder();
            private int batchCount = 0;

            @Override
            public void onNext(CSVImportOuterClass.CSVImportRequest request) {
                try {
                    if (schemaName == null) {
                        schemaName = request.getSchemaName();
                        tableName = request.getTableName();
                        insertSQL.append("INSERT INTO ").append(schemaName).append(".").append(tableName).append(" VALUES ");
                    }

                    insertSQL.append("(");
                    List<String> values = request.getValuesList();
                    for (int i = 0; i < values.size(); i++) {
                        insertSQL.append("'").append(values.get(i).replace("'", "''")).append("'");
                        if (i < values.size() - 1) insertSQL.append(",");
                    }
                    insertSQL.append("),");
                    batchCount++;

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                try {
                    if (insertSQL.length() > 0 && batchCount > 0) {
                        insertSQL.setLength(insertSQL.length() - 1); // remove trailing comma
                        try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + schemaName, DB_USER, DB_PASSWORD);
                             Statement stmt = conn.createStatement()) {

                            stmt.executeUpdate(insertSQL.toString());

                            CSVImportOuterClass.ConfirmationMessage response =
                                    CSVImportOuterClass.ConfirmationMessage.newBuilder()
                                            .setOk(true)
                                            .setMessage("CSV data imported successfully into table '" + tableName + "'.")
                                            .build();
                            responseObserver.onNext(response);
                        }
                    } else {
                        responseObserver.onNext(CSVImportOuterClass.ConfirmationMessage.newBuilder()
                                .setOk(false).setMessage("No data received.").build());
                    }
                } catch (Exception e) {
                    responseObserver.onNext(CSVImportOuterClass.ConfirmationMessage.newBuilder()
                            .setOk(false).setMessage("Import failed: " + e.getMessage()).build());
                }
                responseObserver.onCompleted();
            }
        };
    }
}
