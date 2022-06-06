package com.amazonaws.samples.kinesis2timestream.model;

import java.util.List;

import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;

public class TimestreamRecordConverter {
    public static Record convert(final IotDataBase customObject) {
        if (customObject.getClass().equals(IotData.class)) {
            return convertFromData((IotData) customObject);
        } else {
            throw new RuntimeException("Invalid object type: " + customObject.getClass().getSimpleName());
        }
    }

    private static Record convertFromData(final IotData metric) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("measurement_id")
                        .value(metric.getMeasurementId()).build()  
        );

        String valueStr = convertValueToString(metric.getValue());


        return Record.builder()
                .dimensions(dimensions)
                .measureName(metric.getMeasureName())                
                .measureValueType(MeasureValueType.DOUBLE)
                .measureValue(valueStr) //todo
                .timeUnit(TimeUnit.MILLISECONDS)
                .time(Long.toString(metric.getTime())).build();
    }

    private static String doubleToString(double inputDouble) {
        // Avoid sending -0.0 (negative double) to Timestream - it throws ValidationException
        if (Double.valueOf(-0.0).equals(inputDouble)) {
            return "0.0";
        }
        return Double.toString(inputDouble);
    }

    private static String convertValueToString(Object value) {

        if (value instanceof Number)
            //TODO: perform typecheck
            return doubleToString( ((Number) value).doubleValue() );

        // throw new Exception("Failed to cast");

        return null;
    }

}
