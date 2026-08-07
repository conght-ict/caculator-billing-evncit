package com.evn.billing.worker.service;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.worker.repository.BillingAccountSnapshotRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class CacheAsideSnapshotService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private BillingAccountSnapshotRepository billingAccountSnapshotRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String REDIS_PREFIX = "snapshot:";
    private static final long REDIS_TTL_HOURS = 24;

    /**
     * Fetch the frozen snapshot configuration with Redis Cache-Aside and Circuit Breaker protection.
     */
    @CircuitBreaker(name = "redisCircuitBreaker", fallbackMethod = "fallbackGetSnapshot")
    public BillingConfigSnapshot getSnapshot(String maKhang, String month, int period, int version) {
        String key = REDIS_PREFIX + maKhang + ":" + month + ":" + period + ":v" + version;
        
        // 1. Try fetching from Redis Cache
        try {
            Object cached = redisTemplate.opsForValue().get(key);
            if (cached != null) {
                log.debug("[CACHE-HIT] Loaded snapshot configuration from Redis for Account: {}", maKhang);
                if (cached instanceof String) {
                    return objectMapper.readValue((String) cached, BillingConfigSnapshot.class);
                } else {
                    return objectMapper.convertValue(cached, BillingConfigSnapshot.class);
                }
            }
        } catch (Exception e) {
            log.warn("[CACHE-ERROR] Redis read failed, letting Circuit Breaker handle it: {}", e.getMessage());
            throw new RuntimeException("Redis read failure", e); // Triggers Circuit Breaker fallback
        }

        // 2. Cache-Miss: Load from database
        log.info("[CACHE-MISS] Loading snapshot from Database for Account: {}, Month: {}, Period: {}", maKhang, month, period);
        BillingConfigSnapshot config = loadFromDb(maKhang, month, period, version);

        if (config != null) {
            // 3. Populate Redis Cache
            try {
                String json = objectMapper.writeValueAsString(config);
                redisTemplate.opsForValue().set(key, json, REDIS_TTL_HOURS, TimeUnit.HOURS);
                log.debug("[CACHE-POPULATE] Cached snapshot in Redis with TTL 24h.");
            } catch (Exception e) {
                log.warn("[CACHE-WRITE-ERROR] Failed to save snapshot to Redis: {}", e.getMessage());
            }
        }

        return config;
    }

    /**
     * Fallback method triggered when Redis service is offline or throws errors.
     */
    public BillingConfigSnapshot fallbackGetSnapshot(String maKhang, String month, int period, int version, Throwable t) {
        log.warn("[FALLBACK-ACTIVE] Redis Circuit Breaker open. Fallback to Database for Account: {}. Error: {}", maKhang, t.getMessage());
        return loadFromDb(maKhang, month, period, version);
    }

    private BillingConfigSnapshot loadFromDb(String maKhang, String month, int period, int version) {
        try {
            String json = billingAccountSnapshotRepository.findSnapshotJson(maKhang, month, period, version);
            if (json != null) {
                return objectMapper.readValue(json, BillingConfigSnapshot.class);
            }
        } catch (Exception e) {
            log.error("[DB-ERROR] Failed to retrieve snapshot config from Database: {}", e.getMessage());
        }
        return null;
    }
}
