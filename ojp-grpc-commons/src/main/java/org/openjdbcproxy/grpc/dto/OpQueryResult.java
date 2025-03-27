package org.openjdbcproxy.grpc.dto;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

/**
 * Results of a query statement.
 */
@Builder
@Getter
public class OpQueryResult implements Serializable {
    String resultSetUUID;
    boolean moreData;
    /**
     * Labels for each column returned, only populated in the first block of data returned.
     */
    List<String> labels;
    /**
     * List of rows, each row is an array of objects, each array element is a column of the result set.
     */
    List<Object[]> rows;
}
