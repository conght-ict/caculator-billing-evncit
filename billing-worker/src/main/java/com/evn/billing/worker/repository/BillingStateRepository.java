package com.evn.billing.worker.repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface BillingStateRepository {
    int tryClaimProcessingWorker(String workerNodeId, String maKhang, String month, int period, int claimTimeoutMinutes);
    void seedProcessingStatus(String maKhang, String month, String dtuongQly, int period, String workerNode);
    int updateProcessingStatus(String status, String invoiceId, String errorMsg, Long durationMs, String workerNode, String dtuongQly, String maKhang, String month, int period);
    void updateBookBillingRunProgress(String dtuongQly, String month, int period, int processedDelta, int successDelta, int failedDelta);

    void upsertInvoice(
            String invoiceId,
            String maKhang,
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

    void lockSnapshot(String maKhang, String month, int period, int version);
    void insertOutboxEvent(UUID eventId, String aggregateType, String aggregateId, String eventType, String payloadJson, Timestamp createdAt);

    void batchUpsertInvoices(List<Object[]> invoiceBatch);
    void batchInsertOutbox(List<Object[]> outboxBatch);
    void batchUpsertStatuses(List<Object[]> statusBatch);

    /**
     * Batch claim processing ownership cho toàn bộ danh sách account trong 1 SQL duy nhất.
     * Thay thế N lần tryClaimProcessingWorker() riêng lẻ bằng 1 UPDATE duy nhất.
     * @return danh sách ma_khang thực sự được claim thành công
     */
    List<String> batchClaimProcessingWorkers(List<String> maKhangs, String month, int period, String workerNodeId, int claimTimeoutMinutes);

    List<String> findParentAccountIds(String childAccountId);
    Map<String, Object> findStatusRowForUpdate(String maKhang, String month, int period);
    void updateAccountStatus(String targetStatus, String maKhang, String month, int period);

    void markInvoicesCancelled(String maKhang, String month, int period);
    void setSnapshotsDraft(String maKhang, String month, int period);
    void markAccountCancelled(String maKhang, String month, int period, String message);
    void insertCancelAuditLog(String maKhang, String month, int period,
                              String trangThaiCu, String nguoiHuy,
                              String lyDoHuy, String nguonHuy);

    int countValidatedReadings(String dtuongQly, String month, int period);
    int countByStatuses(String dtuongQly, String month, int period, List<String> statuses);

    List<String> findLockableAccountsForBook(String dtuongQly, String month, int period);
    List<String> findCancelableAccountsForBook(String dtuongQly, String month, int period);
    void lockBookAccounts(String dtuongQly, String month, int period, String targetStatus);

    Integer countTotalAccounts(String dtuongQly, String month, int period);
    Integer countSuccessfulForAutoBatch(String dtuongQly, String month, int period);
    Integer findAutoBatchThreshold();
    String findBookRunStatus(String dtuongQly, String month, int period);

    void approveBookAll(String dtuongQly, String month, int period);
    void approveBookExcluding(String dtuongQly, String month, int period, List<String> excludedAccounts);
    void rejectAccountByCmis(String maKhang, String month, int period, String message);
    List<String> findApprovedAccounts(String dtuongQly, String month, int period);
}
