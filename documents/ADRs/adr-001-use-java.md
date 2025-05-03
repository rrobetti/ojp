# ADR 001: Use Java as the Programming Language

In the context of the OJP project,  
facing the need to implement a JDBC driver and proxy server with high scalability, performance, and compatibility with relational databases,  

we decided for using **Java** as the programming language  
and neglected golang,  

to achieve seamless JDBC integration, leverage Java’s rich ecosystem, and ensure compatibility with relational databases,  
accepting minor runtime memory overhead due to JVM runtime requirements,  

because Java provides native JDBC support, excellent scalability for concurrent systems, and broad community support.

| Status      | APPROVED        |  
|-------------|-----------------| 
| Proposer(s) | Rogerio Robeeti | 
| Proposal date | 01/03/2025      | 
| Approver(s) | Rogerio Robetti |
| Approval date | 01/03/2025      | 

