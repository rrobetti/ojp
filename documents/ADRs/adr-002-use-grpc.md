# ADR 002: Use gRPC for Communication

In the context of the OJP project,  
facing the need for efficient, low-latency, and scalable communication between the JDBC driver and the proxy server,  

we decided for using **gRPC** as the communication protocol  
and neglected REST/JSON APIs and custom communication protocols,  

to achieve high-performance communication with streaming support,  
accepting the extra complexity involving the implementation based on gRPC protocol and Protocol Buffers,  

because gRPC’s HTTP/2 transport enables multiplexed streams and low-latency communication, aligning perfectly with the project’s scalability goals.


| Status      | APPROVED        |  
|-------------|-----------------| 
| Proposer(s) | Rogerio Robeeti | 
| Proposal date | 01/03/2025      | 
| Approver(s) | Rogerio Robetti |
| Approval date | 01/03/2025      | 
