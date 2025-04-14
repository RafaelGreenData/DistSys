package com.dbmapp.client;

import com.dbmapp.grpc.FilteringSortingGrpc;
import com.dbmapp.grpc.FilteringSortingOuterClass.*;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class FilteringSortingClient {

    private final FilteringSortingGrpc.FilteringSortingBlockingStub blockingStub;
    private final FilteringSortingGrpc.FilteringSortingStub asyncStub;

    public FilteringSortingClient(ManagedChannel channel) {
        this.blockingStub = FilteringSortingGrpc.newBlockingStub(channel);
        this.asyncStub = FilteringSortingGrpc.newStub(channel);
    }

    // 🔍 Filter rows using one condition
    public void filterData(String schema, String table, String column, String condition, String value) {
        FilterRequest request = FilterRequest.newBuilder()
                .setSchemaName(schema)
                .setTableName(table)
                .setColumnName(column)
                .setFilterCondition(condition)
                .setValue(value)
                .build();

        try {
            FilterResponse response = blockingStub.filterData(request);
            System.out.println("🔍 Filtered Rows:");
            for (RowData row : response.getFilteredRowsList()) {
                System.out.println(" - " + String.join(" | ", row.getValuesList()));
            }
        } catch (Exception e) {
            System.err.println("❌ Error filtering data: " + e.getMessage());
        }
    }

    // 🔃 Sort table by a given column
    public void sortData(String schema, String table, String column, String order) {
        SortRequest request = SortRequest.newBuilder()
                .setSchemaName(schema)
                .setTableName(table)
                .setColumnName(column)
                .setSortOrder(order)
                .build();

        try {
            SortResponse response = blockingStub.sortData(request);
            System.out.println("🔃 Sorted Rows (" + order + "):");
            for (RowData row : response.getSortedRowsList()) {
                System.out.println(" - " + String.join(" | ", row.getValuesList()));
            }
        } catch (Exception e) {
            System.err.println("❌ Error sorting data: " + e.getMessage());
        }
    }

    // 🔧 Modify the type of a column
    public void modifyColumnType(String schema, String table, String column, String newType) {
        ModifyColumnRequest request = ModifyColumnRequest.newBuilder()
                .setSchemaName(schema)
                .setTableName(table)
                .setColumnName(column)
                .setNewDataType(newType)
                .build();

        try {
            ModifyColumnResponse response = blockingStub.modifyColumnType(request);
            System.out.println((response.getSuccess() ? "✅" : "❌") + " " + response.getMessage());
        } catch (Exception e) {
            System.err.println("❌ Error modifying column type: " + e.getMessage());
        }
    }

    // 🔁 Interactive multi-filter via bi-directional streaming
    public void interactiveFilter(String schema, String table, List<InteractiveFilterRequest> filters) {
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<RowData> responseObserver = new StreamObserver<RowData>() {
            @Override
            public void onNext(RowData row) {
                System.out.println("📥 Row: " + String.join(" | ", row.getValuesList()));
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("❌ Error in stream: " + t.getMessage());
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("✅ Interactive filter stream completed.");
                latch.countDown();
            }
        };

        StreamObserver<InteractiveFilterRequest> requestObserver = asyncStub.interactiveFilter(responseObserver);

        // First message sets the context with schema.table encoded in columnName
        requestObserver.onNext(InteractiveFilterRequest.newBuilder()
                .setColumnName(schema + "." + table)
                .build());

        for (InteractiveFilterRequest filter : filters) {
            requestObserver.onNext(filter);
            try {
                Thread.sleep(500); // Simulate delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        requestObserver.onCompleted();

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
