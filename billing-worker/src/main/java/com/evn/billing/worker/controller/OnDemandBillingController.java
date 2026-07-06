package com.evn.billing.worker.controller;

import com.evn.billing.worker.dto.BillingTaskDto;
import com.evn.billing.worker.service.BillingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    public ResponseEntity<?> calculateImmediate(
            @RequestParam String accountId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Integer version,
            @RequestParam(defaultValue = "SO_DEMAND") String bookId) {
        log.info("[ON-DEMAND-SYNC] Received synchronous immediate calculation request for Account: {}, Month: {}, Book: {}", accountId, month, bookId);
        try {
            BillingTaskDto task = new BillingTaskDto(accountId, bookId, month, version, "on_demand_trace");
            billingService.processBilling(task);
            log.info("[ON-DEMAND-SYNC] Synchronous immediate calculation succeeded for Account: {}", accountId);
            return ResponseEntity.ok("Invoice calculated successfully on demand.");
        } catch (Exception e) {
            log.error("[ON-DEMAND-SYNC] Synchronous immediate calculation failed for Account: {}, Error: {}", accountId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("On-demand calculation failed: " + e.getMessage());
        }
    }

    /**
     * Cancels invoice calculation for a customer and evicts status cache.
     * This resets the calculation status allowing recalculation.
     */
    @org.springframework.web.bind.annotation.DeleteMapping("/cancel")
    public ResponseEntity<?> cancelBilling(
            @RequestParam String accountId,
            @RequestParam String month) {
        log.info("[CANCEL-BILL-API] Received request to cancel billing for Account: {}, Month: {}", accountId, month);
        try {
            billingService.cancelBilling(accountId, month);
            log.info("[CANCEL-BILL-API] Successfully cancelled billing and evicted cache for Account: {}", accountId);
            return ResponseEntity.ok("Billing calculation cancelled successfully. Status set to CANCELLED.");
        } catch (Exception e) {
            log.error("[CANCEL-BILL-API] Failed to cancel billing for Account: {}, Error: {}", accountId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to cancel billing: " + e.getMessage());
        }
    }
}
