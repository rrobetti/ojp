# ADR 003: Use HikariCP as the Connection Pool

In the context of the OJP proxy server,  
facing the need to efficiently manage database connections while maintaining scalability and resource efficiency,  

we decided for using **HikariCP** as the connection pool  
and neglected Apache DBCP and C3P0,  

to achieve high performance, efficient resource utilization, and seamless Java integration,  
accepting the dependency on HikariCP,  

because HikariCP is the fastest JDBC connection pool, with extensive configuration options and a proven track record in high-performance systems.


| Status      | APPROVED        |  
|-------------|-----------------| 
| Proposer(s) | Rogerio Robeeti | 
| Proposal date | 01/03/2025      | 
| Approver(s) | Rogerio Robetti |
| Approval date | 01/03/2025      | 
