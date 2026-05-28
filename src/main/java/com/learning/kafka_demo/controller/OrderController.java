package com.learning.kafka_demo.controller;

import com.learning.kafka_demo.avro.OrderAvro;
import com.learning.kafka_demo.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderProducer orderProducer;

    @PostMapping
    public String placeOrder(@RequestBody OrderRequest request) {
        OrderAvro order = new OrderAvro(
            request.orderId(),
            request.product(),
            request.amount(),
            request.status()
        );

        orderProducer.sendOrder(order);
        return "Order sent: " + order.getOrderId();
    }

    public record OrderRequest(
        String orderId,
        String product,
        int amount,
        String status
    ) {}
}
