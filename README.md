# OJP - Open JDBC Proxy

This is currently oly a POC with very basic functionality implemented. Currently it is possible to perform CRUD operations with very limited types.

## Vision
The goal of OJP project is to provide a free and opensource solution of a relational database agnostic proxy connection pool.

## Target problem
In microservices and/or event driven and/or lambda architectures a common problem is to maintain the number of connections open agaist relational databases to the most efficient number even when the application(s) have to elastic scale drastically, and when that happens the number of connections open against the relational database increases considerably putting pressure in the database and in exctreme scenarios bringing it down.

## The solution
Create a proxy server that holds all the real connections to the database and that maintains such number of connections always under pre-defined settings, allowing the client applications to drastically elastically scale without putting excessive pressure on the relational database(s).

## Components

### ojp-server
The ojp-server is a GRPC server implementation which holds a Hikari connection pool and abstracts the creation and management of connection pools agains one or multiple relational databases.
Provides virtual connections to the jp-jdbc-driver

### ojp-jdbc-driver
The ojp-jdbc-driver is an implementation of the JDBC specification, it connects to the ojp-server via  GRPC protocol sending the statements to be executed against the database and reading the responses. It uses virtual connections provided by the ojp-serve.

### ojp-grpc-commons
The ojp-grpc-commons holds the GRPC contracts shared between the ojp-server and ojp-jdbc-driver.


