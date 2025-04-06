package com.dbmapp;

import com.dbmapp.server.DatabaseManagementServer;
import com.dbmapp.server.TableManagementServer;

import com.dbmapp.client.DatabaseManagementClient;
import com.dbmapp.client.TableManagementClient;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;

public class DbmApp {
    public static void main(String[] args) {
        int port = 50051;

        try {
            // Start gRPC server with both services
            Server server = ServerBuilder.forPort(port)
                    .addService(new DatabaseManagementServer())
                    .addService(new TableManagementServer())
                    .build()
                    .start();

            System.out.println("✅ DBMApp Server started on port " + port);

            // Create gRPC channel
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                    .usePlaintext()
                    .build();

            // Initialize clients
            DatabaseManagementClient dbClient = new DatabaseManagementClient(channel);
            TableManagementClient tableClient = new TableManagementClient(channel);

            Scanner scanner = new Scanner(System.in);
            String activeSchema = null;

            while (true) {
                // MAIN MENU
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
                        String selected = scanner.nextLine();
                        activeSchema = selected;
                        System.out.println("📁 Now working on schema: " + activeSchema);
                        // Go to TABLE MENU
                        boolean inTableMenu = true;
                        while (inTableMenu) {
                            System.out.println("\n📁 Schema: " + activeSchema);
                            System.out.println("1. List Tables");
                            System.out.println("2. View Table Data");
                            System.out.println("3. Drop Table");
                            System.out.println("4. Back to Main Menu");
                            System.out.print("Choose an option: ");
                            int tableChoice = Integer.parseInt(scanner.nextLine());

                            switch (tableChoice) {
                                case 1:
                                    tableClient.getTables(activeSchema);
                                    break;
                                case 2:
                                    System.out.print("Enter table name: ");
                                    String table = scanner.nextLine();
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
