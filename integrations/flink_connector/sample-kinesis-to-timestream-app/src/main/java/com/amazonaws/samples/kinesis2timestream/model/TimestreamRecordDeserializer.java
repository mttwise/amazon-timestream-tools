package com.amazonaws.samples.kinesis2timestream.model;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TimestreamRecordDeserializer implements DeserializationSchema<IotDataBase> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public IotDataBase deserialize(byte[] messageBytes) {
        try {
            return objectMapper.readValue(messageBytes, IotDataBase.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }

    @Override
    public boolean isEndOfStream(IotDataBase nextElement) {
        return false;
    }

    @Override
    public TypeInformation<IotDataBase> getProducedType() {
        return TypeInformation.of(IotDataBase.class);
    }
}
