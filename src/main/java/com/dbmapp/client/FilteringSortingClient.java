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

    // 🔍 Filter rows using one condition (GUI version)
    public FilterResponse filterData(FilterRequest request) {
        return blockingStub.filterData(request);
    }

    // 🔃 Sort table by a given column (GUI version)
    public SortResponse sortData(SortRequest request) {
        return blockingStub.sortData(request);
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
