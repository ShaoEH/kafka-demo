package com.learning.kafka_demo.consumer;

import com.learning.kafka_demo.avro.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderConsumer {
    @KafkaListener(
            topics = "orders",
            groupId = "orders-group-1",
            concurrency = "3"
    )
    public void consume(ConsumerRecord<String, Order> record) {
        log.info("Received order: key={}", record.key());

        // 模擬：order-001 處理失敗
        if ("order-001".equals(record.key())) {
            throw new RuntimeException("Failed to process order: " + record.key());
        }

        log.info("Successfully processed order: key={}", record.key());
    }

    @KafkaListener(
            topics = "orders-dlt",
            groupId = "orders-dlt-group"
    )
    public void consumeDLT(
            ConsumerRecord<String, Object> record,
            @Header(name = "kafka_dlt-exception-message", required = false) String errMsg,
            @Header(name = "kafka_dlt-exception-stacktrace", required = false) byte[] stacktrace
    ) {
        System.out.println("❌ 訊息進入 DLT 的真正原因: " + errMsg);
        if (stacktrace != null) {
            System.out.println("❌ 錯誤堆疊: " + new String(stacktrace));
        }
    }
}
