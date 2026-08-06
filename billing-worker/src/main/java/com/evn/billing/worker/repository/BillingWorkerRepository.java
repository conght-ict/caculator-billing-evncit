package com.evn.billing.worker.repository;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.util.List;

public interface BillingWorkerRepository {

    void saveBillingResults(
            List<InvoiceInsertParam> invoices,
            List<OutboxInsertParam> outboxEvents,
            List<StatusUpdateParam> statuses
    );

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    class InvoiceInsertParam {
        private String id;
        private String customerId;
        private String dtuongQly;
        private String cycleMonth;
        private int period;
        private BigDecimal amountBeforeTax;
        private BigDecimal taxAmount;
        private BigDecimal amountAfterTax;
        private String idempotencyKey;
        private String calculationManifestJson;
        private String refSnapshotId;
        private String maDviqly;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    class OutboxInsertParam {
        private String aggregateType;
        private String aggregateId;
        private String eventType;
        private String payloadJson;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    class StatusUpdateParam {
        private String customerId;
        private String cycleMonth;
        private String dtuongQly;
        private int period;
        private String status;
        private String errorMessage;
        private String invoiceId;
        private long processingTimeMs;
    }
}
