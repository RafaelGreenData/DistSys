package com.dbmapp.server;

import com.dbmapp.grpc.FilteringSortingGrpc;
import com.dbmapp.grpc.FilteringSortingOuterClass.*;

import io.grpc.stub.StreamObserver;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class FilteringSortingServer extends FilteringSortingGrpc.FilteringSortingImplBase {

    private static final String JDBC_URL_PREFIX = "jdbc:mysql://localhost:3306/";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "00bob456";

    // Filter table based on a single condition
    @Override
public void filterData(FilterRequest request, StreamObserver<FilterResponse> responseObserver) {
    String schema = request.getSchemaName();
    String table = request.getTableName();
    String column = request.getColumnName();
    String condition = request.getFilterCondition(); // e.g., '=', 'LIKE', '>', etc.
    String value = request.getValue();

    FilterResponse.Builder responseBuilder = FilterResponse.newBuilder();

    // ✅ Ensure valid operators only
    List<String> validOps = List.of("=", "!=", ">", "<", ">=", "<=", "LIKE");
    if (!validOps.contains(condition.trim())) {
        responseObserver.onError(new IllegalArgumentException("❌ Invalid SQL operator: " + condition));
        return;
    }

    // ✅ Simple, clean SQL without casting
    String sql = String.format("SELECT * FROM %s.%s WHERE `%s` %s ?", schema, table, column, condition);

    try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + schema, DB_USER, DB_PASSWORD);
         PreparedStatement stmt = conn.prepareStatement(sql)) {

        // Automatically works for both strings and numbers
        stmt.setString(1, value);

        ResultSet rs = stmt.executeQuery();
        int columnCount = rs.getMetaData().getColumnCount();

        while (rs.next()) {
            RowData.Builder row = RowData.newBuilder();
            for (int i = 1; i <= columnCount; i++) {
                row.addValues(rs.getString(i));
            }
            responseBuilder.addFilteredRows(row.build());
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();

    } catch (SQLException e) {
        e.printStackTrace();
        responseObserver.onError(e);
    }
}


    // Sort table data by a column
    @Override
    public void sortData(SortRequest request, StreamObserver<SortResponse> responseObserver) {
        String schema = request.getSchemaName();
        String table = request.getTableName();
        String column = request.getColumnName();
        String sortOrder = request.getSortOrder().equalsIgnoreCase("DESC") ? "DESC" : "ASC";

        String query = String.format("SELECT * FROM %s.%s ORDER BY `%s` %s", schema, table, column, sortOrder);
        SortResponse.Builder responseBuilder = SortResponse.newBuilder();

        try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + schema, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            int columnCount = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                RowData.Builder row = RowData.newBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    row.addValues(rs.getString(i));
                }
                responseBuilder.addSortedRows(row.build());
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (SQLException e) {
            e.printStackTrace();
            responseObserver.onError(e);
        }
    }

    // Modify the data type of a column
    @Override
    public void modifyColumnType(ModifyColumnRequest request, StreamObserver<ModifyColumnResponse> responseObserver) {
        String schema = request.getSchemaName();
        String table = request.getTableName();
        String column = request.getColumnName();
        String newType = request.getNewDataType();

        String sql = String.format("ALTER TABLE %s.%s MODIFY COLUMN `%s` %s", schema, table, column, newType);

        try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + schema, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate(sql);

            ModifyColumnResponse response = ModifyColumnResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("✅ Column '" + column + "' modified to type '" + newType + "'.")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (SQLException e) {
            ModifyColumnResponse error = ModifyColumnResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("❌ Error: " + e.getMessage())
                    .build();
            responseObserver.onNext(error);
            responseObserver.onCompleted();
        }
    }

    // Bi-directional streaming: interactive filter
    @Override
    public StreamObserver<InteractiveFilterRequest> interactiveFilter(StreamObserver<RowData> responseObserver) {
        return new StreamObserver<InteractiveFilterRequest>() {

            String schema = null;
            String table = null;
            List<String> conditions = new ArrayList<>();

            @Override
            public void onNext(InteractiveFilterRequest request) {
                if (schema == null || table == null) {
                    String[] parts = request.getColumnName().split("\\.");
                    if (parts.length == 2) {
                        schema = parts[0];
                        table = parts[1];
                        return;
                    } else {
                        responseObserver.onError(new IllegalArgumentException("First message must be schema.table"));
                        return;
                    }
                }

                String column = request.getColumnName();
                String condition = request.getFilterCondition();
                String value = request.getValue();

                conditions.add("`" + column + "` " + condition + " ?");

                String whereClause = String.join(" AND ", conditions);
                String sql = String.format("SELECT * FROM %s.%s WHERE %s", schema, table, whereClause);

                try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + schema, DB_USER, DB_PASSWORD);
                     PreparedStatement stmt = conn.prepareStatement(sql)) {

                    for (int i = 0; i < conditions.size(); i++) {
                        stmt.setString(i + 1, value); // simple handling
                    }

                    ResultSet rs = stmt.executeQuery();
                    int columnCount = rs.getMetaData().getColumnCount();

                    while (rs.next()) {
                        RowData.Builder row = RowData.newBuilder();
                        for (int i = 1; i <= columnCount; i++) {
                            row.addValues(rs.getString(i));
                        }
                        responseObserver.onNext(row.build());
                    }

                } catch (SQLException e) {
                    e.printStackTrace();
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
