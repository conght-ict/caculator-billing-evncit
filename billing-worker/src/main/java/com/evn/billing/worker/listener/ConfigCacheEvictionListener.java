package com.evn.billing.worker.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.redis.core.ScanOptions;

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
            groupId = "config-cache-eviction-group",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
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
                // Xóa toàn bộ snapshot cache dùng SCAN (non-blocking)
                long totalEvicted = 0L;
                List<String> batchKeys = new ArrayList<>();
                try {
                    org.springframework.data.redis.core.Cursor<byte[]> rawCursor =
                            redisTemplate.getConnectionFactory().getConnection().scan(
                                    org.springframework.data.redis.core.ScanOptions.scanOptions()
                                            .match("snapshot:*").count(200).build());
                    while (rawCursor.hasNext()) {
                        batchKeys.add(new String(rawCursor.next()));
                        if (batchKeys.size() >= 200) {
                            totalEvicted += redisTemplate.delete(batchKeys);
                            batchKeys.clear();
                        }
                    }
                    rawCursor.close();
                } catch (Exception scanEx) {
                    log.error("[CACHE-EVICTION] SCAN failed: {}", scanEx.getMessage());
                }
                if (!batchKeys.isEmpty()) {
                    totalEvicted += redisTemplate.delete(batchKeys);
                }
                log.info("[CACHE-EVICTION] Evicted all snapshot config caches via SCAN. Total keys cleared: {}", totalEvicted);
            } else {
                log.warn("[CACHE-EVICTION] Unknown eviction type: {}", evictType);
            }
        } catch (Exception e) {
            log.error("[CACHE-EVICTION] Failed to process cache eviction event: {}", e.getMessage(), e);
        }
    }
}
