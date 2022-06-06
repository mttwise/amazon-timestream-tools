package com.amazonaws.samples.kinesis2timestream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(value = IotData.class, name = "data"),        
})
@Data
public class IotDataBase {

    @JsonProperty(value = "time", required = true)
    private Long time;

    @JsonProperty(value = "measurementId", required = true)
    private String measurementId;

    @JsonProperty(value = "measureName", required = true)
    private String measureName;

}
