package com.evn.billing.worker.listener;

import com.evn.billing.common.dto.BillingTaskDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryQueueListener {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String EXECUTION_TOPIC = "billing-execution-topic";

    /**
     * Consumes task failures and enforces Exponential Backoff using Java 21 Virtual Threads sleep.
     */
    @KafkaListener(
            topics = "billing-retry-topic",
            groupId = "billing-retry-group",
            concurrency = "5" // run multiple consumers in parallel
    )
    public void consumeRetry(BillingTaskDto task, Acknowledgment ack) {
        int attempt = task.getPhienBanTinh();
        long delayMs = getDelayForAttempt(attempt);

        log.info("[RETRY-LISTENER] Received retry for Account: {}, Attempt: {}/{}. Backoff delay: {} seconds.",
                task.getMaKhang(), attempt, 3, delayMs / 1000);

        // Run the backoff sleep. Since spring.threads.virtual.enabled=true is set,
        // this sleep will block the virtual thread, freeing the underlying carrier thread.
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            log.warn("[RETRY-LISTENER] Backoff sleep interrupted: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }

        // Re-dispatch to execution queue to trigger recalculation
        try {
            kafkaTemplate.send(EXECUTION_TOPIC, task.getMaKhang(), task);
            log.info("[RETRY-LISTENER] Re-routed task to Execution Queue for Account: {}", task.getMaKhang());
            ack.acknowledge(); // Commit offset
        } catch (Exception e) {
            log.error("[RETRY-LISTENER] Failed to re-dispatch task: {}", e.getMessage());
        }
    }

    private long getDelayForAttempt(int attempt) {
        switch (attempt) {
            case 1:
                return 30000;  // 30 seconds
            case 2:
                return 120000; // 2 minutes
            case 3:
                return 600000; // 10 minutes
            default:
                return 30000;
        }
    }
}
