package com.dbmapp;

import com.dbmapp.server.DatabaseManagementServer;
import com.dbmapp.server.TableManagementServer;
import com.dbmapp.server.CSVImportServer;
import com.dbmapp.server.FilteringSortingServer;

import com.dbmapp.client.DatabaseManagementClient;
import com.dbmapp.client.TableManagementClient;
import com.dbmapp.client.CSVImportClient;
import com.dbmapp.client.FilteringSortingClient;

import com.dbmapp.grpc.FilteringSortingOuterClass.FilterRequest;
import com.dbmapp.grpc.FilteringSortingOuterClass.SortRequest;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.Scanner;

public class DbmApp {
    public static void main(String[] args) {
        int port = 50051;

        try {
            Server server = ServerBuilder.forPort(port)
                    .addService(new DatabaseManagementServer())
                    .addService(new TableManagementServer())
                    .addService(new CSVImportServer())
                    .addService(new FilteringSortingServer())
                    .build()
                    .start();

            System.out.println("✅ DBMApp Server started on port " + port);

            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                    .usePlaintext()
                    .build();

            DatabaseManagementClient dbClient = new DatabaseManagementClient(channel);
            TableManagementClient tableClient = new TableManagementClient(channel);
            CSVImportClient csvClient = new CSVImportClient(channel, tableClient);
            FilteringSortingClient filterClient = new FilteringSortingClient(channel);


            Scanner scanner = new Scanner(System.in);
            String activeSchema = null;

            while (true) {
                System.out.println("\n📚 DBMApp - Main Menu");
                System.out.println("1. Create Schema");
                System.out.println("2. List Schemas");
                System.out.println("3. Delete Schema");
                System.out.println("4. Use Schema");
                System.out.println("0. Exit");
                System.out.print("Choose an option: ");
                int choice = Integer.parseInt(scanner.nextLine());

                switch (choice) {
                    case 1:
                        System.out.print("Enter schema name to create: ");
                        dbClient.createSchema(scanner.nextLine());
                        break;
                    case 2:
                        dbClient.listSchemas();
                        break;
                    case 3:
                        System.out.print("Enter schema name to delete: ");
                        dbClient.deleteSchema(scanner.nextLine());
                        break;
                    case 4:
                        System.out.print("Enter schema name to use: ");
                        activeSchema = scanner.nextLine();
                        System.out.println("📁 Now working on schema: " + activeSchema);
                        boolean inTableMenu = true;
                        while (inTableMenu) {
                            System.out.println("\n📁 Schema: " + activeSchema);
                            System.out.println("1. List Tables");
                            System.out.println("2. View Table Data");
                            System.out.println("3. Drop Table");
                            System.out.println("4. Import CSV and Create Table");
                            System.out.println("5. Use Table");
                            System.out.println("6. Back to Schema Menu");
                            System.out.print("Choose an option: ");
                            int tableChoice = Integer.parseInt(scanner.nextLine());
                            String table = "";
                            

                            switch (tableChoice) {
                                case 1:
                                    tableClient.getTables(activeSchema);
                                    break;
                                case 2:
                                    System.out.print("Enter table name: ");
                                    table = scanner.nextLine();
                                    System.out.print("Enter number of rows to view: ");
                                    int limit = Integer.parseInt(scanner.nextLine());
                                    tableClient.viewTableData(activeSchema, table, limit);
                                    break;
                                case 3:
                                    System.out.print("Enter table name to drop: ");
                                    String tableToDrop = scanner.nextLine();
                                    tableClient.dropTable(activeSchema, tableToDrop);
                                    break;
                                case 4:
                                    System.out.print("Enter table name to create: ");
                                    String newTable = scanner.nextLine();
                                    System.out.print("Enter full CSV file path: ");
                                    String csvPath = scanner.nextLine();
                                    csvClient.importCSVAndCreateTable(activeSchema, newTable, csvPath);
                                    break;
                                case 5: // Use Table
                                    System.out.print("Enter table name to use: ");
                                    table = scanner.nextLine();

                                    // Get column info
                                    System.out.println("📋 Columns in table '" + table + "':");
                                    tableClient.describeTable(activeSchema, table);  // <-- Make sure this is implemented

                                    boolean inTableOps = true;
                                    while (inTableOps) {
                                        System.out.println("\n⚙️ Operations on table: " + table);
                                        System.out.println("1. Filter table data");
                                        System.out.println("2. Sort table data");
                                        System.out.println("3. Modify column type");
                                        System.out.println("4. Back to Table menu");
                                        System.out.print("Choose an option: ");
                                        int subChoice = Integer.parseInt(scanner.nextLine());

                                        switch (subChoice) {
                                            case 1:
                                                System.out.print("Enter column to filter: ");
                                                String filterCol = scanner.nextLine();
                                                System.out.print("Enter filter condition (e.g. =, >, <, !=): ");
                                                String filterCond = scanner.nextLine();
                                                System.out.print("Enter filter value: ");
                                                String filterVal = scanner.nextLine();

                                                FilterRequest filterRequest = FilterRequest.newBuilder()
                                                        .setSchemaName(activeSchema)
                                                        .setTableName(table)
                                                        .setColumnName(filterCol)
                                                        .setFilterCondition(filterCond)
                                                        .setValue(filterVal)
                                                        .build();

                                                filterClient.filterData(filterRequest);
                                                break;

                                            case 2:
                                                System.out.print("Enter column to sort: ");
                                                String sortCol = scanner.nextLine();
                                                System.out.print("Enter sort order (ASC or DESC): ");
                                                String sortOrder = scanner.nextLine();

                                                SortRequest sortRequest = SortRequest.newBuilder()
                                                        .setSchemaName(activeSchema)
                                                        .setTableName(table)
                                                        .setColumnName(sortCol)
                                                        .setSortOrder(sortOrder)
                                                        .build();

                                                filterClient.sortData(sortRequest);
                                                break;

                                            case 3:
                                                System.out.print("Enter column to modify: ");
                                                String modCol = scanner.nextLine();
                                                System.out.print("Enter new data type (e.g. VARCHAR(255), INT, DATE): ");
                                                String newType = scanner.nextLine();
                                                filterClient.modifyColumnType(activeSchema, table, modCol, newType);
                                                break;

                                            case 4:
                                                inTableOps = false;
                                                break;

                                            default:
                                                System.out.println("⚠️ Invalid option.");
                                        }
                                    }
                                    break;


                                case 8: // Back to Main Menu
                                    inTableMenu = false;
                                    activeSchema = null;
                                    System.out.println("⬅️ Returning to Main Menu...");
                                    break;
                                default:
                                    System.out.println("⚠️ Invalid option.");
                            }
                        }
                        break;
                    case 0:
                        System.out.println("👋 Shutting down...");
                        scanner.close();
                        channel.shutdownNow();
                        server.shutdownNow();
                        return;
                    default:
                        System.out.println("⚠️ Invalid option.");
                }
            }

        } catch (Exception e) {
            System.err.println("❌ Server error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
