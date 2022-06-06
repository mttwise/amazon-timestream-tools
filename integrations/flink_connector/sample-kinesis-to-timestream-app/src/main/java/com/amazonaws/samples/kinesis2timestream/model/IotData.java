package com.amazonaws.samples.kinesis2timestream.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class IotData extends IotDataBase {
    @JsonProperty(value = "value", required = true)
    private Object value;

    @JsonProperty(value = "quality", required = true)
    private Object measureQuality;

    @JsonProperty(value = "measurementType", required = true)
    private String measurementType;
}
