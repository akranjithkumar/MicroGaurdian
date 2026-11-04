package com.microguardian.orderservice.controller;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * OrderController — Simulates a CPU-intensive order processing endpoint.
 * Exposes custom Prometheus metrics for request counting and latency tracking.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final Counter orderCounter;
    private final Counter orderErrorCounter;
    private final Timer orderProcessingTimer;
    private final List<Map<String, Object>> orderStore = Collections.synchronizedList(new ArrayList<>());

    public OrderController(MeterRegistry meterRegistry) {
        this.orderCounter = Counter.builder("orders_processed_total")
                .description("Total number of orders processed")
                .tag("service", "order-service")
                .register(meterRegistry);

        this.orderErrorCounter = Counter.builder("orders_errors_total")
                .description("Total number of order processing errors")
                .tag("service", "order-service")
                .register(meterRegistry);

        this.orderProcessingTimer = Timer.builder("order_processing_duration_seconds")
                .description("Time spent processing an order")
                .tag("service", "order-service")
                .register(meterRegistry);
    }

    /**
     * POST /api/orders — Create a new order.
     * Simulates CPU work via a deliberate busy-wait loop whose duration
     * varies randomly to produce realistic latency distributions.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createOrder(@RequestBody(required = false) Map<String, Object> payload) {
        return orderProcessingTimer.record(() -> {
            try {
                // Simulate CPU-intensive processing (10ms – 200ms of busy work)
                long workDurationMs = ThreadLocalRandom.current().nextLong(10, 200);
                simulateCpuWork(workDurationMs);

                // Randomly fail ~5% of requests to create realistic error rates
                if (ThreadLocalRandom.current().nextDouble() < 0.05) {
                    orderErrorCounter.increment();
                    return ResponseEntity.internalServerError().body(Map.of(
                            "error", "Order processing failed",
                            "timestamp", System.currentTimeMillis()
                    ));
                }

                // Create order record
                String orderId = UUID.randomUUID().toString().substring(0, 8);
                Map<String, Object> order = new LinkedHashMap<>();
                order.put("orderId", orderId);
                order.put("status", "CONFIRMED");
                order.put("items", payload != null ? payload.getOrDefault("items", List.of()) : List.of());
                order.put("total", ThreadLocalRandom.current().nextDouble(10.0, 500.0));
                order.put("timestamp", System.currentTimeMillis());

                orderStore.add(order);
                orderCounter.increment();

                return ResponseEntity.ok(order);

            } catch (Exception e) {
                orderErrorCounter.increment();
                return ResponseEntity.internalServerError().body(Map.of(
                        "error", e.getMessage(),
                        "timestamp", System.currentTimeMillis()
                ));
            }
        });
    }

    /**
     * GET /api/orders — List recent orders.
     */
    @GetMapping
    public ResponseEntity<List<Map<String, Object>>> listOrders(
            @RequestParam(defaultValue = "20") int limit) {
        int fromIndex = Math.max(0, orderStore.size() - limit);
        return ResponseEntity.ok(orderStore.subList(fromIndex, orderStore.size()));
    }

    /**
     * GET /api/orders/health — Simple health check for readiness probes.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "order-service"));
    }

    /**
     * Simulates CPU work by performing math operations for the specified duration.
     * This creates genuine CPU load visible to cAdvisor and Prometheus metrics.
     */
    private void simulateCpuWork(long durationMs) {
        long startTime = System.nanoTime();
        long targetNanos = TimeUnit.MILLISECONDS.toNanos(durationMs);
        double result = 0;

        while ((System.nanoTime() - startTime) < targetNanos) {
            // Perform math operations to generate real CPU load
            for (int i = 0; i < 1000; i++) {
                result += Math.sin(i) * Math.cos(i) * Math.tan(i + 1);
            }
        }

        // Prevent JIT from optimizing away the computation
        if (result == Double.NaN) {
            System.out.println(result);
        }
    }
}
