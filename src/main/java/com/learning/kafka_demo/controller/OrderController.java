package com.learning.kafka_demo.controller;

import com.learning.kafka_demo.avro.Order;
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
        Order order = Order.newBuilder()
                        .setOrderId(request.orderId())
                        .setProduct(request.product())
                        .setAmount(request.amount())
                        .setStatus(request.status())
                        .build();

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
