# ADR 005: Use OpenTelemetry as the observability framework

In the context of the OJP project,  
facing the need to collect, analyze, and visualize metrics and traces to monitor the system's performance and diagnose issues,  

we decided for using the **OpenTelemetry** standard as the observability framework.  
and neglected proprietary solutions like Datadog or New Relic and custom-built telemetry systems,  

to achieve vendor-neutral observability, seamless integration with existing tools, and support for distributed tracing,  
accepting the extra complexity of configuring OpenTelemetry,  

because OpenTelemetry is an open standard with broad community support, extensive ecosystem compatibility, and the ability to export data to multiple backends, offering flexibility and future-proofing.

| Status      | APPROVED        |  
|-------------|-----------------| 
| Proposer(s) | Rogerio Robeeti | 
| Proposal date | 02/05/2025      | 
| Approver(s) | Rogerio Robetti |
| Approval date | 03/05/2025      | 
