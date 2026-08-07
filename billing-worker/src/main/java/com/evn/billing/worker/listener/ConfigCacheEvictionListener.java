package com.evn.billing.worker.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.Set;

@Component
public class ConfigCacheEvictionListener {

    private static final Logger log = LoggerFactory.getLogger(ConfigCacheEvictionListener.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Lắng nghe sự kiện cấu hình thay đổi từ CDC Kafka config-sync-topic.
     * Tự động xóa cache Redis (Cache Invalidation) để đảm bảo đồng nhất dữ liệu.
     */
    @KafkaListener(
            topics = "config-sync-topic",
            groupId = "config-cache-eviction-group"
    )
    public void listenConfigChangeEvents(String message) {
        log.info("[CACHE-EVICTION] Received config sync event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String evictType = (String) event.get("evictType"); // SNAPSHOT | TARIFF | ALL
            
            if ("SNAPSHOT".equalsIgnoreCase(evictType)) {
                String maKhang = (String) event.get("maKhang");
                String month = (String) event.get("billingCycleMonth");
                Integer period = (Integer) event.get("period");
                if (period == null) period = 1;
                
                String cacheKey = "snapshot:" + maKhang + ":" + month + ":" + period;
                Boolean deleted = redisTemplate.delete(cacheKey);
                log.info("[CACHE-EVICTION] Evicted snapshot cache for key: {} (Result: {})", cacheKey, deleted);
                
            } else if ("TARIFF".equalsIgnoreCase(evictType) || "ALL".equalsIgnoreCase(evictType)) {
                // Xóa toàn bộ snapshot cache để tránh sai cước khi biểu giá thay đổi
                Set<String> keys = redisTemplate.keys("snapshot:*");
                if (keys != null && !keys.isEmpty()) {
                    Long count = redisTemplate.delete(keys);
                    log.info("[CACHE-EVICTION] Evicted all snapshot config caches. Total keys cleared: {}", count);
                } else {
                    log.info("[CACHE-EVICTION] No snapshot caches to evict.");
                }
            } else {
                log.warn("[CACHE-EVICTION] Unknown eviction type: {}", evictType);
            }
        } catch (Exception e) {
            log.error("[CACHE-EVICTION] Failed to process cache eviction event: {}", e.getMessage(), e);
        }
    }
}
