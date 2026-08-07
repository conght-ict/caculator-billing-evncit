package com.evn.billing.worker.controller;

import com.evn.billing.common.dto.CalculateImmediateRequest;
import com.evn.billing.common.dto.CancelBillingRequest;
import com.evn.billing.common.dto.LockBillingRequest;
import com.evn.billing.common.dto.BookProgressRequest;
import com.evn.billing.common.dto.AccountsByStatusRequest;
import com.evn.billing.worker.service.BillingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/billing")
public class OnDemandBillingController {

    private static final Logger log = LoggerFactory.getLogger(OnDemandBillingController.class);

    @Autowired
    private BillingService billingService;

    /**
     * Exposes On-Demand Synchronous calculation API.
     * CMIS or mediation-service can call this synchronously to force immediate calculation
     * when background automatic computation is not yet completed.
     */
    @PostMapping("/calculate-immediate")
    public ResponseEntity<?> calculateImmediate(@RequestBody CalculateImmediateRequest request) {
        String maKhang = request.getMaKhang();
        String month = request.getThangChuKy();

        log.info("[ON-DEMAND-SYNC] Received synchronous immediate calculation request for Account: {}, Month: {}", maKhang, month);
        try {
            billingService.calculateImmediate(
                    maKhang, month, request.getKyChot(), request.getPhienBan(),
                    request.getDtuongQly(), request.getTriggeredBy());
            log.info("[ON-DEMAND-SYNC] Synchronous immediate calculation succeeded for Account: {}", maKhang);
            return ResponseEntity.ok("Invoice calculated successfully on demand.");
        } catch (Exception e) {
            log.error("[ON-DEMAND-SYNC] Synchronous immediate calculation failed for Account: {}, Error: {}", maKhang, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("On-demand calculation failed: " + e.getMessage());
        }
    }

    /**
     * Cancels invoice calculation for a customer and evicts status cache.
     * This resets the calculation status allowing recalculation.
     */
    @PostMapping("/cancel")
    public ResponseEntity<?> cancelBilling(@RequestBody CancelBillingRequest request) {
        String maKhang = request.getMaKhang();
        String month = request.getThangChuKy();
        Integer period = request.getKyChot() != null ? request.getKyChot() : 1;

        log.info("[CANCEL-BILL-API] Received request to cancel billing for Account: {}, Month: {}, Period: {}", maKhang, month, period);
        try {
            billingService.cancelBilling(maKhang, month, period);
            log.info("[CANCEL-BILL-API] Successfully cancelled billing and evicted cache for Account: {}", maKhang);
            return ResponseEntity.ok("Billing calculation cancelled successfully. Status set to CANCELLED.");
        } catch (Exception e) {
            log.error("[CANCEL-BILL-API] Failed to cancel billing for Account: {}, Error: {}", maKhang, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to cancel billing: " + e.getMessage());
        }
    }

    /**
     * Locks or updates invoice status (e.g. to E_INVOICE_ISSUED) when CMIS issues E-Invoice.
     */
    @PostMapping("/lock")
    public ResponseEntity<?> lockBilling(@RequestBody LockBillingRequest request) {
        String maKhang = request.getMaKhang();
        String month = request.getThangChuKy();
        Integer period = request.getKyChot() != null ? request.getKyChot() : 1;
        String status = request.getTrangThai() != null ? request.getTrangThai() : "LOCKED";

        log.info("[LOCK-BILL-API] Received request to lock billing status for Account: {}, Month: {}, Period: {}, Target Status: {}", 
                maKhang, month, period, status);
        try {
            billingService.lockBilling(maKhang, month, period, status);
            return ResponseEntity.ok("Billing status successfully updated to " + status + ".");
        } catch (Exception e) {
            log.error("[LOCK-BILL-API] Failed to lock billing status: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to lock billing status: " + e.getMessage());
        }
    }

    @PostMapping("/book-progress")
    public ResponseEntity<Map<String, Object>> getBookProgress(@RequestBody BookProgressRequest request) {
        String dtuongQly = request.getDtuongQly();
        String month = request.getThangChuKy();
        Integer period = request.getKyChot() != null ? request.getKyChot() : 1;

        log.info("[PROGRESS-API] Fetching summary progress for Book: {}, Month: {}, Period: {}", dtuongQly, month, period);
        Map<String, Object> progress = billingService.getBookProgress(dtuongQly, month, period);
        return ResponseEntity.ok(progress);
    }

    @PostMapping("/accounts-by-status")
    public ResponseEntity<Page<com.evn.billing.common.domain.AccountBillingStatus>> getAccountsByStatus(@RequestBody AccountsByStatusRequest request) {
        String dtuongQly = request.getDtuongQly();
        String month = request.getThangChuKy();
        Integer period = request.getKyChot() != null ? request.getKyChot() : 1;
        String statusesStr = request.getStatuses();
        List<String> statusesList = statusesStr != null ? Arrays.asList(statusesStr.split(",")) : java.util.Collections.emptyList();
        int page = request.getPage() != null ? request.getPage() : 0;
        int size = request.getSize() != null ? request.getSize() : 10;

        log.info("[STATUS-LIST-API] Fetching accounts by status for Book: {}, Month: {}, Period: {}, Statuses: {}, Page: {}, Size: {}", 
                dtuongQly, month, period, statusesList, page, size);
        Page<com.evn.billing.common.domain.AccountBillingStatus> result = 
                billingService.getAccountsByStatus(dtuongQly, month, period, statusesList, PageRequest.of(page, size));
        return ResponseEntity.ok(result);
    }
}
