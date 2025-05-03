# ADR 004: Implement JDBC Interface

In the context of the OJP JDBC driver,  
facing the need to integrate seamlessly with Java applications and provide database-agnostic connectivity,  

we decided for implementing the **JDBC interface**  
and neglected creating a custom API or using an ODBC interface,  

to achieve compatibility with existing Java applications and minimize migration effort,  
accepting the complexity of implementing advanced JDBC features,  

because the JDBC interface is a well-established standard that ensures wide adoption and simplifies integration with multiple relational databases.


| Status      | APPROVED        |  
|-------------|-----------------| 
| Proposer(s) | Rogerio Robeeti | 
| Proposal date | 01/03/2025      | 
| Approver(s) | Rogerio Robetti |
| Approval date | 01/03/2025      | 
