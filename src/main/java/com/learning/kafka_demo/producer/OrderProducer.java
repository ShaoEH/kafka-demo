package com.learning.kafka_demo.producer;

import com.learning.kafka_demo.avro.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {
    private final KafkaTemplate<String, Order> kafkaTemplate;


    public void sendOrder(Order order) {
        kafkaTemplate.executeInTransaction(operations -> {
            log.info("Sending order: {}", order);
            operations.send("orders", order.getOrderId().toString(), order);
            log.info("Order sent successfully: {}", order.getOrderId());
            return true;
        });
    }
}
