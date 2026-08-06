package com.evn.billing.worker.repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface BillingStateRepository {
    int tryClaimProcessingWorker(String workerNodeId, String accountId, String month, int period, int claimTimeoutMinutes);
    void seedProcessingStatus(String accountId, String month, String dtuongQly, int period, String workerNode);
    int updateProcessingStatus(String status, String invoiceId, String errorMsg, Long durationMs, String workerNode, String dtuongQly, String accountId, String month, int period);
    void updateBookBillingRunProgress(String dtuongQly, String month, int period, int processedDelta, int successDelta, int failedDelta);

    void upsertInvoice(
            String invoiceId,
            String accountId,
            String dtuongQly,
            String month,
            BigDecimal totalBeforeTax,
            BigDecimal taxAmount,
            BigDecimal totalAfterTax,
            String idempotencyKey,
            String manifestJson,
            boolean isProrated,
            String refSnapshot,
            String status,
            String maDviqly,
            Timestamp createdAt,
            Timestamp updatedAt);

    void lockSnapshot(String accountId, String month, int period, int version);
    void insertOutboxEvent(UUID eventId, String aggregateType, String aggregateId, String eventType, String payloadJson, Timestamp createdAt);

    void batchUpsertInvoices(List<Object[]> invoiceBatch);
    void batchInsertOutbox(List<Object[]> outboxBatch);
    void batchUpsertStatuses(List<Object[]> statusBatch);

    List<String> findParentAccountIds(String childAccountId);
    Map<String, Object> findStatusRowForUpdate(String accountId, String month, int period);
    void updateAccountStatus(String targetStatus, String accountId, String month, int period);

    void markInvoicesCancelled(String accountId, String month, int period);
    void setSnapshotsDraft(String accountId, String month, int period);
    void markAccountCancelled(String accountId, String month, int period, String message);

    int countValidatedReadings(String dtuongQly, String month, int period);
    int countByStatuses(String dtuongQly, String month, int period, List<String> statuses);

    List<String> findLockableAccountsForBook(String dtuongQly, String month, int period);
    void lockBookAccounts(String dtuongQly, String month, int period, String targetStatus);

    Integer countTotalAccounts(String dtuongQly, String month, int period);
    Integer countSuccessfulForAutoBatch(String dtuongQly, String month, int period);
    Integer findAutoBatchThreshold();
    String findBookRunStatus(String dtuongQly, String month, int period);

    void approveBookAll(String dtuongQly, String month, int period);
    void approveBookExcluding(String dtuongQly, String month, int period, List<String> excludedAccounts);
    void rejectAccountByCmis(String accountId, String month, int period, String message);
    List<String> findApprovedAccounts(String dtuongQly, String month, int period);
}
