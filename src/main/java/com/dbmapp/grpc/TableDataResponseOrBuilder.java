// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: TableManagement.proto

package com.dbmapp.grpc;

public interface TableDataResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:tablemanagement.TableDataResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated string columnNames = 1;</code>
   */
  java.util.List<java.lang.String>
      getColumnNamesList();
  /**
   * <code>repeated string columnNames = 1;</code>
   */
  int getColumnNamesCount();
  /**
   * <code>repeated string columnNames = 1;</code>
   */
  java.lang.String getColumnNames(int index);
  /**
   * <code>repeated string columnNames = 1;</code>
   */
  com.google.protobuf.ByteString
      getColumnNamesBytes(int index);

  /**
   * <code>repeated .tablemanagement.RowData rows = 2;</code>
   */
  java.util.List<com.dbmapp.grpc.RowData> 
      getRowsList();
  /**
   * <code>repeated .tablemanagement.RowData rows = 2;</code>
   */
  com.dbmapp.grpc.RowData getRows(int index);
  /**
   * <code>repeated .tablemanagement.RowData rows = 2;</code>
   */
  int getRowsCount();
  /**
   * <code>repeated .tablemanagement.RowData rows = 2;</code>
   */
  java.util.List<? extends com.dbmapp.grpc.RowDataOrBuilder> 
      getRowsOrBuilderList();
  /**
   * <code>repeated .tablemanagement.RowData rows = 2;</code>
   */
  com.dbmapp.grpc.RowDataOrBuilder getRowsOrBuilder(
      int index);
}
