package com.microguardian.gatewayservice.controller;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * GatewayController — API Gateway that routes requests to downstream
 * order-service and inventory-service. Tracks aggregate request metrics
 * for Prometheus scraping.
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final RestTemplate restTemplate;
    private final String orderServiceUrl;
    private final String inventoryServiceUrl;

    private final Counter gatewayRequestCounter;
    private final Counter gatewayErrorCounter;
    private final Timer gatewayLatencyTimer;

    public GatewayController(
            RestTemplate restTemplate,
            @Value("${microservices.order-service-url}") String orderServiceUrl,
            @Value("${microservices.inventory-service-url}") String inventoryServiceUrl,
            MeterRegistry meterRegistry) {
        this.restTemplate = restTemplate;
        this.orderServiceUrl = orderServiceUrl;
        this.inventoryServiceUrl = inventoryServiceUrl;

        this.gatewayRequestCounter = Counter.builder("gateway_requests_total")
                .description("Total requests routed through the gateway")
                .tag("service", "gateway-service")
                .register(meterRegistry);

        this.gatewayErrorCounter = Counter.builder("gateway_errors_total")
                .description("Total gateway routing errors")
                .tag("service", "gateway-service")
                .register(meterRegistry);

        this.gatewayLatencyTimer = Timer.builder("gateway_request_duration_seconds")
                .description("End-to-end gateway request duration")
                .tag("service", "gateway-service")
                .register(meterRegistry);
    }

    /**
     * POST /api/gateway/orders — Route order creation to order-service.
     */
    @PostMapping("/gateway/orders")
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, Object>> createOrder(
            @RequestBody(required = false) Map<String, Object> payload) {
        return gatewayLatencyTimer.record(() -> {
            gatewayRequestCounter.increment();
            try {
                ResponseEntity<Map> response = restTemplate.postForEntity(
                        orderServiceUrl + "/api/orders", payload, Map.class);
                return ResponseEntity.status(response.getStatusCode())
                        .body((Map<String, Object>) response.getBody());
            } catch (Exception e) {
                gatewayErrorCounter.increment();
                return ResponseEntity.internalServerError().body(Map.of(
                        "error", "Failed to route to order-service: " + e.getMessage(),
                        "service", "gateway"
                ));
            }
        });
    }

    /**
     * GET /api/gateway/inventory/{productId} — Route inventory lookup.
     */
    @GetMapping("/gateway/inventory/{productId}")
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, Object>> getInventory(@PathVariable String productId) {
        return gatewayLatencyTimer.record(() -> {
            gatewayRequestCounter.increment();
            try {
                ResponseEntity<Map> response = restTemplate.getForEntity(
                        inventoryServiceUrl + "/api/inventory/" + productId, Map.class);
                return ResponseEntity.status(response.getStatusCode())
                        .body((Map<String, Object>) response.getBody());
            } catch (Exception e) {
                gatewayErrorCounter.increment();
                return ResponseEntity.internalServerError().body(Map.of(
                        "error", "Failed to route to inventory-service: " + e.getMessage(),
                        "service", "gateway"
                ));
            }
        });
    }

    /**
     * GET /api/gateway/health — Aggregate health check.
     */
    @GetMapping("/gateway/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("status", "UP");
        health.put("service", "gateway-service");

        try {
            restTemplate.getForEntity(orderServiceUrl + "/api/orders/health", Map.class);
            health.put("order-service", "UP");
        } catch (Exception e) {
            health.put("order-service", "DOWN");
        }

        try {
            restTemplate.getForEntity(inventoryServiceUrl + "/api/inventory/health", Map.class);
            health.put("inventory-service", "UP");
        } catch (Exception e) {
            health.put("inventory-service", "DOWN");
        }

        return ResponseEntity.ok(health);
    }
}
