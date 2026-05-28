package com.learning.kafka_demo.consumer;

import com.learning.kafka_demo.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
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
    public void consumeDLT(ConsumerRecord<String, Order> record) {
        log.info("Received order from DLT: key={}", record.key());
    }
}
