NoSQL Record Database Implementation
====================================

Overview
--------

A Java-based NoSQL database implementation that provides MongoDB-like functionality for storing and managing documents in a schema-less format. This implementation focuses on thread safety, concurrent operations, and efficient data management using modern Java features.

Table of Contents
-----------------

-   [Features](#features)
-   [Technical Stack](#technical-stack)
-   [Project Structure](#project-structure)
-   [Getting Started](#getting-started)
-   [Usage Guide](#usage-guide)
-   [Command Reference](#command-reference)
-   [Architecture](#architecture)
-   [Implementation Details](#implementation-details)
-   [Contributing](#contributing)

Features
--------

-   Schema-less document storage
-   Thread-safe operations
-   Concurrent document processing
-   Persistent storage with serialization
-   Command-line interface
-   ACID compliance for basic operations
-   Support for complex queries
-   Automatic data persistence
-   Real-time data access

Technical Stack
---------------

-   Java 17+
-   Concurrent utilities from java.util.concurrent
-   Java Stream API
-   Java NIO for file operations
-   Custom serialization implementation
-   Logger framework

Project Structure
-----------------
    
    com.recorddb/
    ├── core/
    │   ├── DatabaseOperations.java
    │   └── NoSQLDatabase.java
    ├── model/
    │   └── Document.java
    ├── command/
    │   └── CommandExecutor.java
    ├── util/
    │   ├── DocumentSerializer.java
    │   └── ValidationUtil.java
    └── server/
        └── App.java

Getting Started
---------------

### Prerequisites

-   Java Development Kit (JDK) 17 or higher
-   Gradle 7.0+ or Maven 3.6+
-   512MB minimum available storage space

### Installation

1.  Clone the repository:
   `git clone https://github.com/yourusername/nosql-record-db.git`

1.  Navigate to project directory:
    `cd nosql-record-db`

1.  Build the project:
    `gradle build`

1.  Run the application:
    `gradle run`

Usage Guide
-----------

### Starting the Database

Run the main application class:

    public static void main(String[] args) {
        DatabaseOperations database = new NoSQLDatabase();
        CommandExecutor executor = new CommandExecutor(database);
        // ... rest of the implementation
    }

### Basic Operations

Insert a Single Document
------------------------
    INSERT_ONE { _id: 101, name: JohnDoe, age: 30, location: NY }

Insert Multiple Documents
-------------------------
    INSERT_MANY [
        { _id: 201, book: 1984, author: George Orwell },
        { _id: 202, book: Brave New World, author: Aldous Huxley }
    ]

Find Documents
--------------
    FIND { author: George Orwell }    

Delete Documents
----------------
    DELETE { author: George Orwell }

Command Reference
-----------------

### INSERT_ONE

-   Format: `INSERT_ONE { _id: <value>, <key>: <value>, ... }`
-   Returns: `SUCCESS` or `INVALID_COMMAND` or `ID_CONFLICT`
-   Example: `INSERT_ONE { _id: 101, name: JohnDoe, age: 30 }`

### INSERT_MANY

-   Format: `INSERT_MANY [{ _id: <value>, ... }, { _id: <value>, ... }]`
-   Returns: Comma-separated list of results
-   Example: `INSERT_MANY [{ _id: 201, name: Alice }, { _id: 202, name: Bob }]`

### FIND

-   Format: `FIND { <key>: <value>, ... }`
-   Returns: Comma-separated list of matching document IDs
-   Example: `FIND { department: HR }`

### DELETE

-   Format: `DELETE { <key>: <value>, ... }`
-   Returns: Number of deleted documents
-   Example: `DELETE { department: HR }`

### STOP

-   Format: `STOP`
-   Returns: "Adios!"

### PURGE_AND_STOP

-   Format: `PURGE_AND_STOP`
-   Returns: "PURGED, Adios!"

Architecture
------------

### Core Components

1.  **Document Model**
    -   Implements `Serializable`
    -   Thread-safe internal structure
    -   Key-value pair storage
2.  **Database Operations**
    -   Asynchronous operation handling
    -   Concurrent access management
    -   ACID compliance
3.  **Command Executor**
    -   Command parsing and validation
    -   Error handling
    -   Result formatting
4.  **Storage Manager**
    -   File-based persistence
    -   Serialization/Deserialization
    -   Data integrity

### Implementation Details

Thread Safety
-------------

-   ReadWriteLock for concurrent access
-   ConcurrentHashMap for in-memory storage
-   Atomic operations for critical sections

Concurrency
-----------

`private final ExecutorService executorService;
private final ReadWriteLock rwLock;
private final ConcurrentMap<String,  Document> memoryStore;`

Serialization
-------------

`public void serialize(Document document, String filePath) {
    try (ObjectOutputStream oos = new ObjectOutputStream(
            new BufferedOutputStream(Files.newOutputStream(Paths.get(filePath))))) {
        oos.writeObject(document);
    }
}`

Advanced Features
-----------------

### Daemon Threads

The implementation uses daemon threads for background operations:

`Thread thread = new Thread(r);
thread.setDaemon(true);
return thread;`

### Stream API Usage

Efficient data processing using streams:

`documents.parallelStream()
    .map(doc -> CompletableFuture.supplyAsync(() -> {
        // Processing logic
    }))
    .collect(Collectors.toList());`

### Synchronization

Proper resource management:

`try {
    rwLock.writeLock().lock();
    // Critical section
} finally {
    rwLock.writeLock().unlock();
}`

Contributing
------------

### Development Setup

1.  Fork the repository
2.  Create a feature branch
3.  Implement changes
4.  Submit pull request

### Code Style

-   Follow Java naming conventions
-   Include relevant unit tests
-   Document public APIs
-   Use proper exception handling

### Testing

Run the test suite:
`gradle test`

License
-------

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

Acknowledgments
---------------

-   Inspired by MongoDB's document storage model
-   Built using modern Java concurrency features
-   Implements industry-standard ACID properties


### For more information, please contact <adityaraj.pilot@hotmail.com>
