package com.learning.kafka_demo.controller;

import com.learning.kafka_demo.model.Order;
import com.learning.kafka_demo.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderProducer orderProducer;

    @PostMapping
    public String placeOrder(@RequestBody Order order) {
        orderProducer.sendOrder("orders", order);
        return "Order sent: " + order.getOrderId();
    }
}
