package com.microguardian.inventoryservice.controller;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * InventoryController — Simulates a memory-intensive inventory lookup service.
 * Maintains an in-memory cache that grows with requests, producing visible
 * memory pressure in container metrics.
 */
@RestController
@RequestMapping("/api/inventory")
public class InventoryController {

    private final Counter lookupCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;
    private final Timer lookupTimer;
    private final AtomicInteger cacheSize;

    // In-memory inventory cache — deliberately grows to simulate memory pressure
    private final Map<String, Map<String, Object>> inventoryCache = new ConcurrentHashMap<>();

    // Pre-seeded product catalog
    private static final String[] PRODUCT_NAMES = {
            "Wireless Headphones", "Mechanical Keyboard", "USB-C Hub",
            "Monitor Stand", "Webcam HD", "Laptop Sleeve", "Power Bank",
            "Bluetooth Speaker", "Smart Watch", "Portable SSD",
            "Ergonomic Mouse", "Desk Lamp", "Cable Organizer", "Phone Mount",
            "Screen Protector", "Tablet Case", "Memory Card", "Card Reader",
            "Charging Pad", "Ethernet Adapter"
    };

    public InventoryController(MeterRegistry meterRegistry) {
        this.lookupCounter = Counter.builder("inventory_lookups_total")
                .description("Total inventory lookups performed")
                .tag("service", "inventory-service")
                .register(meterRegistry);

        this.cacheHitCounter = Counter.builder("inventory_cache_hits_total")
                .description("Cache hits during inventory lookup")
                .tag("service", "inventory-service")
                .register(meterRegistry);

        this.cacheMissCounter = Counter.builder("inventory_cache_misses_total")
                .description("Cache misses during inventory lookup")
                .tag("service", "inventory-service")
                .register(meterRegistry);

        this.lookupTimer = Timer.builder("inventory_lookup_duration_seconds")
                .description("Time spent on inventory lookups")
                .tag("service", "inventory-service")
                .register(meterRegistry);

        this.cacheSize = new AtomicInteger(0);
        Gauge.builder("inventory_cache_size", cacheSize, AtomicInteger::get)
                .description("Current size of the inventory cache")
                .tag("service", "inventory-service")
                .register(meterRegistry);

        // Pre-seed some inventory data
        seedInventory();
    }

    /**
     * GET /api/inventory/{productId} — Look up a product by ID.
     * Simulates memory-intensive operations by expanding cached data per request.
     */
    @GetMapping("/{productId}")
    public ResponseEntity<Map<String, Object>> getProduct(@PathVariable String productId) {
        return lookupTimer.record(() -> {
            lookupCounter.increment();

            // Simulate lookup delay (5ms – 50ms)
            simulateMemoryWork(ThreadLocalRandom.current().nextLong(5, 50));

            Map<String, Object> product = inventoryCache.get(productId);
            if (product != null) {
                cacheHitCounter.increment();
                return ResponseEntity.ok(product);
            }

            // Cache miss — generate and store a new product
            cacheMissCounter.increment();
            product = generateProduct(productId);
            inventoryCache.put(productId, product);
            cacheSize.set(inventoryCache.size());

            return ResponseEntity.ok(product);
        });
    }

    /**
     * GET /api/inventory — List all cached inventory items.
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> listInventory(
            @RequestParam(defaultValue = "50") int limit) {
        List<Map<String, Object>> items = inventoryCache.values().stream()
                .limit(limit)
                .toList();
        return ResponseEntity.ok(Map.of(
                "items", items,
                "totalCached", inventoryCache.size()
        ));
    }

    /**
     * POST /api/inventory/bulk-lookup — Perform bulk inventory lookups.
     * This endpoint is intentionally memory-heavy to create visible resource usage.
     */
    @PostMapping("/bulk-lookup")
    public ResponseEntity<Map<String, Object>> bulkLookup(
            @RequestBody(required = false) Map<String, Object> payload) {
        return lookupTimer.record(() -> {
            int count = 20;
            if (payload != null && payload.containsKey("count")) {
                count = Math.min(((Number) payload.get("count")).intValue(), 100);
            }

            List<Map<String, Object>> results = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                String productId = "PROD-" + ThreadLocalRandom.current().nextInt(1, 500);
                lookupCounter.increment();

                Map<String, Object> product = inventoryCache.get(productId);
                if (product != null) {
                    cacheHitCounter.increment();
                } else {
                    cacheMissCounter.increment();
                    product = generateProduct(productId);
                    inventoryCache.put(productId, product);
                }
                results.add(product);
            }
            cacheSize.set(inventoryCache.size());

            // Simulate memory-heavy aggregation
            simulateMemoryWork(ThreadLocalRandom.current().nextLong(20, 100));

            return ResponseEntity.ok(Map.of(
                    "results", results,
                    "cacheSize", inventoryCache.size()
            ));
        });
    }

    /**
     * GET /api/inventory/health — Health check for readiness probes.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "inventory-service",
                "cacheSize", inventoryCache.size()
        ));
    }

    private void seedInventory() {
        for (int i = 1; i <= 50; i++) {
            String productId = "PROD-" + i;
            inventoryCache.put(productId, generateProduct(productId));
        }
        cacheSize.set(inventoryCache.size());
    }

    private Map<String, Object> generateProduct(String productId) {
        Map<String, Object> product = new LinkedHashMap<>();
        product.put("productId", productId);
        product.put("name", PRODUCT_NAMES[ThreadLocalRandom.current().nextInt(PRODUCT_NAMES.length)]);
        product.put("price", Math.round(ThreadLocalRandom.current().nextDouble(5.0, 300.0) * 100.0) / 100.0);
        product.put("stock", ThreadLocalRandom.current().nextInt(0, 500));
        product.put("warehouse", "WH-" + ThreadLocalRandom.current().nextInt(1, 5));
        product.put("lastUpdated", System.currentTimeMillis());

        // Add some bulk data to increase memory footprint
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tags.add("tag-" + UUID.randomUUID().toString().substring(0, 8));
        }
        product.put("tags", tags);

        return product;
    }

    /**
     * Simulates memory-intensive work by allocating temporary data structures.
     */
    private void simulateMemoryWork(long durationMs) {
        long startTime = System.nanoTime();
        long targetNanos = TimeUnit.MILLISECONDS.toNanos(durationMs);
        List<byte[]> tempAllocations = new ArrayList<>();

        while ((System.nanoTime() - startTime) < targetNanos) {
            // Allocate small byte arrays to create memory pressure
            tempAllocations.add(new byte[1024]);
            if (tempAllocations.size() > 100) {
                tempAllocations.clear(); // Prevent OOM, but GC pressure remains
            }
        }
    }
}
