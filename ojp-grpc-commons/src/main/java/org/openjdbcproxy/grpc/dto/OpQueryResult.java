package org.openjdbcproxy.grpc.dto;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * Results of a query statement.
 */
@Builder
@Getter
public class OpQueryResult implements Serializable {
    //TODO it is not possible to return this because it is not serializable and it is expensive to translate it for all columns
    //need to find another way maybe return basic metadata
    //ResultSetMetaData resultSetMetaData;
    List<Object[]> rows;
}
