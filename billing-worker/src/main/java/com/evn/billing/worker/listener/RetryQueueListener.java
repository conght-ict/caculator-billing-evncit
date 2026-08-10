package com.evn.billing.worker.listener;

import com.evn.billing.common.dto.BillingTaskDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * RetryQueueListener — Non-blocking retry consumer.
 *
 * DESIGN DECISION: Sử dụng notBefore timestamp trong message header để kiểm tra
 * xem đã đến lúc retry chưa mà không cần Thread.sleep (gây rebalance).
 *
 * Nếu chưa đến lúc retry: ACK ngay, task sẽ được SelfHealingService.processScheduledRetries()
 * xử lý qua DB-based delayed scheduler (chạy mỗi 60 giây).
 *
 * Nếu đến lúc retry: re-route sang billing-execution-topic.
 */
@Component
@Slf4j
public class RetryQueueListener {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String EXECUTION_TOPIC = "billing-execution-topic";

    @KafkaListener(
            topics = "billing-retry-topic",
            groupId = "billing-retry-group",
            concurrency = "2"
    )
    public void consumeRetry(ConsumerRecord<String, BillingTaskDto> record, Acknowledgment ack) {
        BillingTaskDto task = record.value();
        if (task == null) {
            ack.acknowledge();
            return;
        }

        // Kiểm tra notBefore header để quyết định có re-route ngay không
        long notBefore = 0L;
        org.apache.kafka.common.header.Header header = record.headers().lastHeader("notBefore");
        if (header != null) {
            try {
                notBefore = java.nio.ByteBuffer.wrap(header.value()).getLong();
            } catch (Exception ignored) {}
        }

        long now = System.currentTimeMillis();
        if (now < notBefore) {
            // Chưa đến thời điểm retry. ACK message (không xử lý lại ngay).
            // SelfHealingService.processScheduledRetries() sẽ tự sweep qua DB sau.
            log.info("[RETRY-LISTENER] Task for Account: {} not yet due (notBefore={}). Delegating to DB scheduler.",
                    task.getMaKhang(), notBefore);
            ack.acknowledge();
            return;
        }

        // Đến lúc retry → re-route sang execution queue
        try {
            kafkaTemplate.send(EXECUTION_TOPIC, task.getMaKhang(), task);
            log.info("[RETRY-LISTENER] Re-routed task to Execution Queue for Account: {}, Attempt: {}",
                    task.getMaKhang(), task.getPhienBanTinh());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[RETRY-LISTENER] Failed to re-dispatch task for Account: {}: {}", task.getMaKhang(), e.getMessage());
            // Không ACK → Kafka sẽ redeliver
        }
    }
}
