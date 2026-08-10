package com.evn.billing.worker.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.evn.billing.worker.service.BillingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * BillingOperationsListener — Nhận lệnh điều hành cước từ Portal/CMIS qua Kafka.
 *
 * Áp dụng @RetryableTopic với 3 lần retry (backoff 2s, 10s, 30s) trước khi
 * đẩy vào DLT topic để tránh mất operation khi DB tạm thời lỗi.
 */
@Component
@Slf4j
public class BillingOperationsListener {

    @Autowired
    private BillingService billingService;

    @Autowired
    private com.evn.billing.worker.repository.BillingStateRepository billingStateRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 2000, multiplier = 5, maxDelay = 30000),
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = {Exception.class}
    )
    @KafkaListener(
            topics = "billing-operations-topic",
            groupId = "billing-operations-group",
            containerFactory = "operationsKafkaListenerContainerFactory"
    )
    public void listenBillingOperations(String message) {
        log.info("[OPERATIONS-KAFKA] Received operation event: {}", message);
        Map<String, Object> event;
        try {
            event = objectMapper.readValue(message, Map.class);
        } catch (Exception e) {
            log.error("[OPERATIONS-KAFKA] Cannot parse message as JSON. Skipping: {}", message);
            return; // Không retry tin nhắn không parse được
        }

        String op = (String) event.get("operationType");
        String maKhang = (String) event.get("maKhang");
        String dtuongQly = (String) event.get("dtuongQly");
        String month = (String) event.get("billingCycleMonth");
        int period = ((Number) event.get("period")).intValue();
        
        String nguoiHuy = (String) event.getOrDefault("nguoiHuy", "KAFKA_SYSTEM");
        String lyDoHuy = (String) event.getOrDefault("lyDoHuy", null);

        try {
            if ("LOCK_ACCOUNTS".equalsIgnoreCase(op)) {
                billingService.lockBookBilling(dtuongQly, month, period, "LOCKED");
                log.info("[OPERATIONS-KAFKA] Successfully locked billing for Book: {}", dtuongQly);

            } else if ("APPROVE_BOOK".equalsIgnoreCase(op)) {
                List<String> excludedAccounts = (List<String>) event.get("excludedAccounts");
                billingService.approveBookBilling(dtuongQly, month, period, excludedAccounts);
                log.info("[OPERATIONS-KAFKA] Successfully approved billing for Book: {} excluding {}", dtuongQly, excludedAccounts);

            } else if ("LOCK_CMIS".equalsIgnoreCase(op)) {
                billingService.lockBilling(maKhang, month, period, "SUCCESS_CMIS");
                log.info("[OPERATIONS-KAFKA] Successfully locked CMIS billing for account: {}", maKhang);

            } else if ("CANCEL_BILLING".equalsIgnoreCase(op)) {
                if (maKhang != null && !maKhang.isEmpty()) {
                    billingService.cancelBilling(maKhang, month, period, nguoiHuy, lyDoHuy, "KAFKA");
                    log.info("[OPERATIONS-KAFKA] Successfully cancelled billing for account: {}", maKhang);
                } else if (dtuongQly != null && !dtuongQly.isEmpty()) {
                    billingService.cancelBookBilling(dtuongQly, month, period);
                    log.info("[OPERATIONS-KAFKA] Successfully cancelled billing for Book: {}", dtuongQly);
                } else {
                    log.warn("[OPERATIONS-KAFKA] CANCEL_BILLING received but both maKhang and dtuongQly are empty. Skipping.");
                }

            } else if ("ISSUE_E_INVOICE".equalsIgnoreCase(op)) {
                billingService.lockBilling(maKhang, month, period, "E_INVOICE_ISSUED");
                log.info("[OPERATIONS-KAFKA] Successfully issued E-Invoice for account: {}", maKhang);

            } else {
                log.warn("[OPERATIONS-KAFKA] Unknown operation type: {}. Skipping.", op);
            }

        } catch (Exception e) {
            // Ném exception ra ngoài để @RetryableTopic bắt và retry
            log.error("[OPERATIONS-KAFKA] Operation {} failed for Account: {}/Book: {}: {}", op, maKhang, dtuongQly, e.getMessage(), e);
            throw new RuntimeException("[OPERATIONS-KAFKA] Failed to execute operation: " + op, e);
        }
    }

    /**
     * DLT Handler — Xử lý khi operation đã retry 3 lần mà vẫn thất bại.
     * Ghi log CRITICAL và lưu thông tin lỗi vào nhat_ky_huy_tinh.
     */
    @DltHandler
    public void handleDlt(String message,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
        log.error("[OPERATIONS-DLT] CRITICAL: Operation permanently failed after all retries! " +
                "Topic: {}, Message: {}, Error: {}", topic, message, exceptionMessage);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String maKhang = (String) event.getOrDefault("maKhang", "UNKNOWN");
            String month   = (String) event.getOrDefault("billingCycleMonth", "UNKNOWN");
            int period     = event.get("period") != null ? ((Number) event.get("period")).intValue() : 0;
            billingStateRepository.insertCancelAuditLog(maKhang, month, period,
                    null, "DLT_SYSTEM", "Operation failed after retries: " + exceptionMessage, "OPERATIONS_DLT");
        } catch (Exception e) {
            log.warn("[OPERATIONS-DLT] Cannot parse DLT message for audit log: {}", e.getMessage());
        }
    }
}
