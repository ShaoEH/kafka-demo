package com.learning.kafka_demo.producer;

import com.learning.kafka_demo.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {
    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Transactional
    public void sendOrder(String topic, Order order) {
        kafkaTemplate.send(topic, order.getOrderId(), order);
        if (order.getAmount() > 10000) {
            throw new RuntimeException("Order amount exceeds limit, rolling back!");
        }
        log.info("Sent order: {}", order.getOrderId());
    }
}
