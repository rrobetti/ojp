package org.openjdbcproxy.grpc.dto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
public class Parameter implements Serializable {
    private Integer index;
    private ParameterType type;
    private List<Object> values;
}
