package com.evn.billing.worker.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.evn.billing.worker.service.BillingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class BillingOperationsListener {

    private static final Logger log = LoggerFactory.getLogger(BillingOperationsListener.class);

    @Autowired
    private BillingService billingService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "billing-operations-topic",
            groupId = "billing-operations-group"
    )
    public void listenBillingOperations(String message) {
        log.info("[OPERATIONS-KAFKA] Received operation event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String op = (String) event.get("operationType");
            String maKhang = (String) event.get("maKhang");
            String dtuongQly = (String) event.get("dtuongQly");
            String month = (String) event.get("billingCycleMonth");
            int period = ((Number) event.get("period")).intValue();

            if ("LOCK_ACCOUNTS".equalsIgnoreCase(op)) {
                billingService.lockBookBilling(dtuongQly, month, period, "LOCKED");
                log.info("[OPERATIONS-KAFKA] Successfully locked calculated billing for Book: {}", dtuongQly);
            } else if ("APPROVE_BOOK".equalsIgnoreCase(op)) {
                java.util.List<String> excludedAccounts = (java.util.List<String>) event.get("excludedAccounts");
                billingService.approveBookBilling(dtuongQly, month, period, excludedAccounts);
                log.info("[OPERATIONS-KAFKA] Successfully approved calculated billing for Book: {} excluding {}", dtuongQly, excludedAccounts);
            } else if ("LOCK_CMIS".equalsIgnoreCase(op)) {
                billingService.lockBilling(maKhang, month, period, "SUCCESS_CMIS");
                log.info("[OPERATIONS-KAFKA] Successfully approved calculated billing anomaly for account: {}", maKhang);
            } else if ("CANCEL_BILLING".equalsIgnoreCase(op)) {
                billingService.cancelBilling(maKhang, month, period);
                log.info("[OPERATIONS-KAFKA] Successfully cancelled calculated billing for account: {}", maKhang);
            } else if ("ISSUE_E_INVOICE".equalsIgnoreCase(op)) {
                billingService.lockBilling(maKhang, month, period, "E_INVOICE_ISSUED");
                log.info("[OPERATIONS-KAFKA] Successfully issued E-Invoice and locked account: {}", maKhang);
            } else {
                log.warn("[OPERATIONS-KAFKA] Unknown operation type: {}", op);
            }
        } catch (Exception e) {
            log.error("[OPERATIONS-KAFKA] Failed to process operation message: {}", e.getMessage(), e);
        }
    }
}
