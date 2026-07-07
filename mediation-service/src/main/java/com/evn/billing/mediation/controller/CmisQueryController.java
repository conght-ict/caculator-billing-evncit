package com.evn.billing.mediation.controller;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.mediation.repository.BillInvoiceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/invoices")
public class CmisQueryController {

    private static final Logger log = LoggerFactory.getLogger(CmisQueryController.class);

    @Autowired
    private BillInvoiceRepository billInvoiceRepository;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Exposes query endpoint for CMIS.
     * CMIS checks here first; if 404 (Not Found), mediation-service automatically
     * triggers on-demand calculation on billing-worker as fallback (Scenario B).
     */
    @GetMapping
    public ResponseEntity<?> getInvoice(
            @RequestParam String accountId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Integer period) {
        
        Optional<BillInvoice> invoiceOpt = billInvoiceRepository.findByAccountIdAndBillingCycleMonthAndPeriod(accountId, month, period);
        if (invoiceOpt.isPresent()) {
            return ResponseEntity.ok(invoiceOpt.get());
        }

        // Scenario B: Fallback On-Demand Sync Calculation
        try {
            log.info("[FALLBACK] Invoice missing for Account: {}, Month: {}, Period: {}. Triggering immediate calculation...", accountId, month, period);
            String calcUrl = "http://localhost:8081/api/v1/billing/calculate-immediate?accountId=" + accountId + "&month=" + month + "&period=" + period;
            
            ResponseEntity<String> response = restTemplate.postForEntity(calcUrl, null, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                // Re-fetch invoice after successful calculation
                invoiceOpt = billInvoiceRepository.findByAccountIdAndBillingCycleMonthAndPeriod(accountId, month, period);
                if (invoiceOpt.isPresent()) {
                    return ResponseEntity.ok(invoiceOpt.get());
                }
            }
            return ResponseEntity.status(500).body("On-demand calculation succeeded but invoice was not found in DB.");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("On-demand calculation fallback failed: " + e.getMessage());
        }
    }
}
