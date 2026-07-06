package com.evn.billing.worker.listener;

import com.evn.billing.worker.dto.BillingTaskDto;
import com.evn.billing.worker.service.BillingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import java.nio.ByteBuffer;
import java.util.ArrayList;

@Component
public class BillingKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(BillingKafkaListener.class);

    @Autowired
    private BillingService billingService;

    @Autowired
    private org.springframework.data.redis.core.StringRedisTemplate redisTemplate;

    /**
     * Kafka Listener executing calculation tasks with Non-Blocking Retries.
     * Uses 4 attempts with exponential backoff.
     * Manual offset commits are performed only after successful database saving.
     */
    @KafkaListener(
            topics = "billing-execution-topic",
            groupId = "billing-worker-group",
            containerFactory = "kafkaBatchListenerContainerFactory"
    )
    public void listenBillingTasks(
            java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<String, BillingTaskDto>> records, 
            Acknowledgment ack) {
        long t3Receive = System.currentTimeMillis();
        try {
            log.info("Received billing task batch of size: {}", records.size());
            
            java.util.List<BillingTaskDto> tasks = new ArrayList<>();
            long totalIngest = 0;
            long totalQueue = 0;
            long totalCount = 0;

            for (org.apache.kafka.clients.consumer.ConsumerRecord<String, BillingTaskDto> record : records) {
                BillingTaskDto task = record.value();
                if (task != null) {
                    tasks.add(task);
                    
                    org.apache.kafka.common.header.Header h1 = record.headers().lastHeader("t1_ingest");
                    org.apache.kafka.common.header.Header h2 = record.headers().lastHeader("t2_send");
                    if (h1 != null && h2 != null) {
                        try {
                            long t1Ingest = ByteBuffer.wrap(h1.value()).getLong();
                            long t2Send = ByteBuffer.wrap(h2.value()).getLong();
                            totalIngest += (t2Send - t1Ingest);
                            totalQueue += (t3Receive - t2Send);
                            totalCount++;
                        } catch (Exception e) {
                            // Ignore
                        }
                    }
                }
            }

            billingService.processBillingBatch(tasks);
            
            // Commit offset thủ công sau khi lưu Database cả lô thành công
            ack.acknowledge();
            
            long t4Commit = System.currentTimeMillis();
            try {
                redisTemplate.opsForValue().increment("benchmark:total_count", tasks.size());
                long calcLatency = t4Commit - t3Receive;
                redisTemplate.opsForValue().increment("benchmark:total_latency_calc", calcLatency);
                
                if (totalCount > 0) {
                    redisTemplate.opsForValue().increment("benchmark:total_latency_ingest", totalIngest);
                    redisTemplate.opsForValue().increment("benchmark:total_latency_queue", totalQueue);
                    
                    long totalE2e = totalIngest + totalQueue + (calcLatency * totalCount);
                    redisTemplate.opsForValue().increment("benchmark:total_latency_e2e", totalE2e);
                    
                    long avgE2e = totalE2e / totalCount;
                    redisTemplate.opsForList().rightPush("benchmark:latencies:e2e", String.valueOf(avgE2e));
                    redisTemplate.opsForList().trim("benchmark:latencies:e2e", -1000, -1);
                }
            } catch (Exception e) {
                // Ignore
            }
        } catch (Exception e) {
            log.error("Batch calculation failed: {}", e.getMessage(), e);
            throw new RuntimeException("Failing task batch to trigger Kafka retry backoff", e);
        }
    }

    private void saveMetricsToRedis(long ingest, long queue, long calc, long e2e) {
        try {
            redisTemplate.opsForValue().increment("benchmark:total_latency_e2e", e2e);
            redisTemplate.opsForValue().increment("benchmark:total_latency_ingest", ingest);
            redisTemplate.opsForValue().increment("benchmark:total_latency_queue", queue);
            redisTemplate.opsForValue().increment("benchmark:total_latency_calc", calc);
            redisTemplate.opsForValue().increment("benchmark:total_count");

            // Push to rolling queue (limit to latest 1000 records)
            redisTemplate.opsForList().rightPush("benchmark:latencies:e2e", String.valueOf(e2e));
            redisTemplate.opsForList().trim("benchmark:latencies:e2e", -1000, -1);
        } catch (Exception e) {
            log.warn("Failed to save metrics to Redis: {}", e.getMessage());
        }
    }

    /**
     * Handles tasks that have permanently failed all retry attempts.
     */
    @DltHandler
    public void handleDlt(BillingTaskDto task, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("ALERT: Billing task permanently failed. Sent to DLT. Account: {}, Topic: {}", 
                task.getAccountId(), topic);
    }
}
