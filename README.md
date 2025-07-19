# OJP - Open JDBC Proxy

**A JDBC Driver and Layer 7 Proxy Server to decouple applications from relational database connection management.**

[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://buymeacoffee.com/wqoejbve8z)

## Status: Alpha version in development

This project is currently in active development and should be considered alpha software. Features and APIs may change without notice.

## Quick Start

### Using Docker (Fastest Setup)

1. **Start the OJP Server:**
   ```bash
   docker run --rm -d -p 1059:1059 rrobetti/ojp:0.0.4-alpha
   ```

2. **Add the JDBC Driver to your project:**
   ```xml
   <dependency>
       <groupId>org.openjdbcproxy</groupId>
       <artifactId>ojp-jdbc-driver</artifactId>
       <version>0.0.4-alpha</version>
   </dependency>
   ```

3. **Update your connection URL:**
   ```java
   // Before: 
   String url = "jdbc:postgresql://user@localhost/mydb";
   
   // After:
   String url = "jdbc:ojp_[localhost:1059]postgresql://user@localhost/mydb";
   ```

4. **Disable your existing connection pool** in your application - OJP handles connection pooling for you.

## Overview

### Questions we aim to answer:
- **How to autoscale our applications without overloading our relational database(s) with new connections?**
- **How to replace native JDBC drivers seamlessly?**
- **How to support multiple relational databases at once?**

### The Problem
In modern architectures (microservices, event-driven systems, serverless), applications often maintain too many database connections, leading to resource waste and potential database outages during elastic scaling.

### The Solution
OJP provides intelligent connection management by only allocating real database connections when operations are performed, not when Connection objects are created. This prevents database overload while maintaining application scalability.

## Architecture

<img src="https://raw.githubusercontent.com/Open-JDBC-Proxy/ojp/main/documents/designs/ojp_high_level_design.gif" alt="OJP High Level Design" />

**Key Features:**
- **Smart Connection Management**: Real connections allocated only when needed
- **Elastic Scalability**: Applications scale without pressuring the database
- **Database Agnostic**: Supports any database with JDBC drivers
- **gRPC Protocol**: Efficient multiplexed communication
- **HikariCP Integration**: Battle-tested connection pooling
- **Open Source**: Free to use, modify, and distribute

## Documentation

- **[Architectural Decision Records (ADRs)](https://github.com/Open-JDBC-Proxy/ojp/tree/main/documents/ADRs)** - Design decisions and rationale
- **[Framework Integration Guide](https://github.com/Open-JDBC-Proxy/ojp/tree/main/documents/java-frameworks)** - Setup with Spring Boot, Quarkus, and Micronaut
- **[Connection Pool Configuration](https://github.com/Open-JDBC-Proxy/ojp/blob/main/documents/configuration/CONNECTION_POOL_CONFIG.md)** - Advanced pool tuning options
- **[Server Configuration](https://github.com/Open-JDBC-Proxy/ojp/blob/main/documents/configuration/ojp-server-configuration.md)** - OJP server setup and options
- **[Telemetry and Observability](https://github.com/Open-JDBC-Proxy/ojp/tree/main/documents/telemetry)** - Monitoring and metrics integration

## Components

### ojp-server
gRPC server managing HikariCP connection pools with support for multiple databases. Provides virtual connections to clients while controlling real database connections.

### ojp-jdbc-driver
JDBC-compliant driver that connects to ojp-server via gRPC, enabling seamless database operations without direct connection management.

### ojp-grpc-commons
Shared gRPC contracts and communication protocols between server and driver components.

## Contributing & Developer Guide

### Prerequisites
- Java 11+ (tested with Java 11, 17, 21, 22)
- Maven 3.6+
- Docker (optional, for testing with databases)

### Clone and Build

1. **Clone the repository:**
   ```bash
   git clone https://github.com/rrobetti/ojp.git
   cd ojp
   ```

2. **Build all modules:**
   ```bash
   mvn clean install -DskipTests
   ```

3. **Start the OJP server (required for tests):**
   ```bash
   mvn verify -pl ojp-server -Prun-ojp-server
   ```

4. **Run tests:**
   ```bash
   mvn test
   ```
   
   Optional flags:
   - `-DdisablePostgresTests` - Skip tests requiring PostgreSQL

### Database Setup for Testing
See [Local Database Setup Guide](https://github.com/Open-JDBC-Proxy/ojp/blob/main/documents/environment-setup/run-local-databases.md) for running test databases locally.

### Contributing Guidelines
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes and add tests
4. Ensure all tests pass: `mvn test`
5. Submit a pull request with a clear description

For detailed contribution guidelines, see the main project at [Open-JDBC-Proxy/ojp](https://github.com/Open-JDBC-Proxy/ojp).

## Vision
Provide a free and open-source solution for a relational database-agnostic proxy connection pool, designed for microservices, event-driven architectures, and serverless environments while maintaining high scalability and performance.

## Feature Implementation Status

| Feature | Status | Notes |
|---------|---------|-------|
| Basic CRUD operations | ✅ | Complete |
| Streamed result set reading | ✅ | Complete |
| BLOB support | ✅ | Complete |
| Transactions support | ✅ | Complete |
| Binary Stream support | ✅ | Complete |
| ResultSet metadata | ✅ | Complete |
| Statement/PreparedStatement features | ✅ | Complete |
| Connection advanced features | ✅ | Complete |
| OpenTelemetry implementation | ✅ | Complete |
| Circuit Breaker | ✅ | Complete |
| Docker image | ✅ | Complete |
| Spring Boot/Spring Data support | ✅ | Complete |
| Micronaut support | ✅ | Complete |
| Quarkus support | ✅ | Complete |
| Configurable data sources | ✅ | Complete |
| CLOB support | ❌ | Planned |
| BLOB/CLOB advanced features | ❌ | Planned |
| Slow queries segregation | ❌ | Planned |
| RAFT consensus POC | ❌ | Research phase |
| RAFT connection balancing | ❌ | Research phase |

**Legend:** ✅ Complete | ❌ Not started | 🕓 In progress

### Future Feature Candidates
Query Routing, Sharding, Query Caching, Read/Write Splitting, Multi-Cloud/Distributed Clustering, Authentication Integration, Advanced Security Features, Failover and Automatic Replication Awareness, Helidon support.

## Partners

<a href="https://www.linkedin.com/in/devsjava/">
<img width="150px" height="150px" src="https://raw.githubusercontent.com/Open-JDBC-Proxy/ojp/main/documents/images/comunidade_brasil_jug.jpeg" alt="Comunidade Brasil JUG" />
</a>
<a href="https://github.com/switcherapi">
<img width="180px" src="https://raw.githubusercontent.com/Open-JDBC-Proxy/ojp/main/documents/images/switcherapi_grey.png" alt="SwitcherAPI" />
</a>

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

**Note:** This repository has been moved to [Open-JDBC-Proxy/ojp](https://github.com/Open-JDBC-Proxy/ojp) for active development. This repository serves as a reference and documentation hub.