package com.learning.kafka_demo.avro;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecordBase;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class OrderAvro_HandWriteTest extends SpecificRecordBase {
    public static final Schema SCHEMA$ = SchemaBuilder.record("OrderAvro")
            .namespace("com.learning.kafka_demo.avro")
            .fields()
                .requiredString("orderId")
                .requiredString("product")
                .requiredInt("amount")
                .requiredString("status")
            .endRecord();

    private String orderId;
    private String product;
    private int amount;
    private String status;

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        return switch (field) {
            case 0 -> orderId;
            case 1 -> product;
            case 2 -> amount;
            case 3 -> status;
            default -> throw new org.apache.avro.AvroRuntimeException("Bad index: " + field);
        };
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0 -> orderId = value.toString();
            case 1 -> product = value.toString();
            case 2 -> amount = (Integer) value;
            case 3 -> status = value.toString();
            default -> throw new org.apache.avro.AvroRuntimeException("Bad index: " + field);
        }
    }

    @Override
    public String toString() {
        return "OrderAvro{orderId='" + orderId + "', product='" + product +
                "', amount=" + amount + ", status='" + status + "'}";
    }
}
