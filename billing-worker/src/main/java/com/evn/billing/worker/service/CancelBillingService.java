package com.evn.billing.worker.service;

import com.evn.billing.worker.repository.BillingStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * CancelBillingService — Tách biệt logic hủy cước khỏi BillingService
 * để giải quyết vấn đề Spring AOP self-invocation (@Transactional bị bypass).
 *
 * Mỗi lần gọi cancelBilling() từ BillingService.cancelBookBilling() đều đi qua
 * Spring proxy → @Transactional được kích hoạt → lỗi 1 account không rollback cả book.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class CancelBillingService {

    private final BillingStateRepository billingStateRepository;

    /**
     * Hủy cước cho 1 tài khoản trong transaction độc lập.
     * Gọi billingService param để cập nhật cache (tránh circular dependency qua constructor).
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void cancelBilling(String maKhang, String month, int period,
                              BillingService billingService,
                              String nguoiHuy, String lyDoHuy, String nguonHuy) throws Exception {
        Map<String, Object> row;
        try {
            row = billingStateRepository.findStatusRowForUpdate(maKhang, month, period);
        } catch (Exception e) {
            throw new NoSuchElementException(
                    "Không tìm thấy thông tin cước đã tính cho khách hàng: "
                    + maKhang + ", kỳ: " + month + ", đợt: " + period);
        }

        String dtuongQly = (String) row.get("dtuong_qly");
        String oldStatus = (String) row.get("trang_thai");

        if ("CANCELLED".equals(oldStatus)) {
            log.info("[CANCEL-BILL] Account {} already CANCELLED for kỳ: {}, đợt: {}", maKhang, month, period);
            return;
        }

        if ("LOCKED".equals(oldStatus) || "E_INVOICE_ISSUED".equals(oldStatus)) {
            throw new IllegalStateException(
                    "Hóa đơn của khách hàng " + maKhang + " kỳ " + month + " đợt " + period
                    + " đã phát hành HĐĐT hoặc đã khóa. Không thể hủy cước trực tiếp!");
        }

        log.info("[CANCEL-BILL] Cancelling billing for Account: {}, Month: {}, Period: {}, Book: {}, Old Status: {}",
                maKhang, month, period, dtuongQly, oldStatus);

        // Append-Only Rule: đánh dấu CANCELLED thay vì xóa
        billingStateRepository.markInvoicesCancelled(maKhang, month, period);
        log.info("[CANCEL-BILL] Marked invoices as CANCELLED in 'hoa_don' table.");

        billingStateRepository.setSnapshotsDraft(maKhang, month, period);
        log.info("[CANCEL-BILL] Reset snapshot status to DRAFT to allow CMIS updates.");

        String ghiChu = lyDoHuy != null ? lyDoHuy : "Hủy hóa đơn bởi " + nguonHuy;
        billingStateRepository.markAccountCancelled(maKhang, month, period, ghiChu);
        billingStateRepository.insertCancelAuditLog(maKhang, month, period, oldStatus, nguoiHuy, lyDoHuy, nguonHuy);

        // Cập nhật Redis + local cache (non-transactional — lỗi cache không rollback DB)
        billingService.updateCancelStatusCaches(dtuongQly, maKhang, month, period);

        // Cập nhật tiến độ sổ cước
        if ("SUCCESS".equals(oldStatus) || "SUCCESS_CMIS".equals(oldStatus) || "ANOMALY".equals(oldStatus)) {
            billingService.updateBookBillingRunProgress(dtuongQly, month, period, -1, -1, 0);
        } else if ("FAILED".equals(oldStatus)) {
            billingService.updateBookBillingRunProgress(dtuongQly, month, period, -1, 0, -1);
        }
        log.info("[CANCEL-BILL] Billing run progress decremented.");

        // Cascading Cancellation: hủy các account cha phụ thuộc vào account này
        List<String> parentAccountIds = billingStateRepository.findParentAccountIds(maKhang);
        for (String parentId : parentAccountIds) {
            log.info("[CASCADING-CANCEL] Parent '{}' depends on cancelled child '{}'. Triggering cascade.", parentId, maKhang);
            try {
                // Gọi lại qua this (cùng bean CancelBillingService) — Spring proxy hoạt động đúng
                cancelBilling(parentId, month, period, billingService, nguoiHuy, lyDoHuy, "CASCADE");
            } catch (NoSuchElementException e) {
                log.info("[CASCADING-CANCEL] Parent '{}' not yet calculated. Skipping.", parentId);
            }
        }
    }
}
