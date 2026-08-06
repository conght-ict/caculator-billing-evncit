package com.evn.billing.mediation.controller;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.dto.InvoiceQueryRequest;
import com.evn.billing.mediation.service.CmisQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/invoices")
public class CmisQueryController {

    @Autowired
    private CmisQueryService cmisQueryService;

    @PostMapping
    public ResponseEntity<?> getInvoice(@RequestBody InvoiceQueryRequest request) {
        String accountId = request.getMaKhang();
        String month = request.getThangChuKy();
        int period = request.getKyChot() != null ? request.getKyChot() : 1;

        Optional<BillInvoice> invoiceOpt = cmisQueryService.getOrCalculateInvoice(accountId, month, period);
        if (invoiceOpt.isPresent()) {
            return ResponseEntity.ok(invoiceOpt.get());
        }
        return ResponseEntity.status(500).body("Invoice not found and on-demand calculation fallback failed.");
    }
}
