# DistSys
# DBMApp - Distributed Database Management Application

This repository contains the source code for **DBMApp**, a distributed system project developed using gRPC and Java for the **Distributed Systems** module at the National College of Ireland (HDip in Computing).

## 📚 Project Description

**DBMApp** is a distributed database management application that allows users to remotely interact with and manage databases. It uses gRPC-based communication between services and provides a GUI controller for a user-friendly experience. The project was proposed independently as a personal initiative and approved by the module lecturer.

---

## 💡 Application Domain

This project focuses on **Distributed Database Management**.

DBMApp was proposed to explore distributed systems in the context of database administration. It aims to:
- Support efficient and remote database control
- Provide tools for importing, filtering, and managing data
- Simulate real-world use cases for analysts and businesses needing scalable solutions

---

## 🧩 System Architecture

- **gRPC Server**: Hosts all services and handles incoming requests
- **Java Swing GUI**: Provides a user interface to interact with services
- **MySQL Database**: Stores all schemas, tables, and data
- **Protocol Buffers (.proto)**: Define communication protocols
- **gRPC Clients**: GUI or CLI that sends requests to the server

---

## 🛠️ Technologies Used

- Java 17
- Apache NetBeans IDE 21
- gRPC & Protocol Buffers
- MySQL
- Java Swing (GUI)
- Maven

---

## 🔐 Key Features

| Feature | Description |
|--------|-------------|
| **gRPC Communication** | All services use gRPC for efficient, remote method invocation |
| **Service Discovery** | Services can be discovered by the client dynamically |
| **Authentication** | Basic authentication implemented for secure access |
| **Logging** | Logs service usage and errors for analysis |
| **Graphical Interface** | Java Swing-based GUI for managing and interacting with services |
| **All 4 RPC Types** | Unary, Server Streaming, Client Streaming, Bi-directional Streaming |

---

## 📦 Services Overview

### 1. Database Management Service
- Create, list, and delete schemas

### 2. Table Management Service
- Retrieve tables, delete tables, preview data

### 3. CSV Import Service
- Retrieve headers, import CSV data into tables

### 4. Filtering and Sorting Service
- Multi-condition filtering, sorting, and data type modification

---

## 📁 Project Structure

```bash
DBMApp/
├── client/              # Java Swing GUI
├── server/              # gRPC service implementations
├── proto/               # .proto definitions
├── logs/                # Service logs
├── README.md            # Project overview
└── pom.xml              # Maven dependencies
```
