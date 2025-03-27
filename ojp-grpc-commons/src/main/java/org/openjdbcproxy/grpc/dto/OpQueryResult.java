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
    List<Object[]> rows;
}
