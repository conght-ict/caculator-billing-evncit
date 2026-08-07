package com.evn.billing.mediation.service;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.dto.CalculateImmediateRequest;
import com.evn.billing.mediation.repository.BillInvoiceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.util.Optional;

@Service
public class CmisQueryService {

    private static final Logger log = LoggerFactory.getLogger(CmisQueryService.class);

    @Autowired
    private BillInvoiceRepository billInvoiceRepository;

    @Value("${billing.worker.url:http://localhost:8081/worker}")
    private String billingWorkerUrl;

    private final RestTemplate restTemplate;

    public CmisQueryService() {
        org.springframework.http.client.SimpleClientHttpRequestFactory factory = new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);
        factory.setReadTimeout(15000);
        this.restTemplate = new RestTemplate(factory);
    }

    public Optional<BillInvoice> getInvoice(String maKhang, String month, int period) {
        return billInvoiceRepository.findByMaKhangAndThangChuKyAndKyChot(maKhang, month, period);
    }

    public Optional<BillInvoice> getOrCalculateInvoice(String maKhang, String month, int period) {
        Optional<BillInvoice> invoiceOpt = billInvoiceRepository.findByMaKhangAndThangChuKyAndKyChot(maKhang, month, period);
        if (invoiceOpt.isPresent()) {
            return invoiceOpt;
        }

        // Fallback: trigger on-demand sync calculation
        try {
            log.info("[FALLBACK] Invoice missing for Account: {}, Month: {}, Period: {}. Triggering immediate calculation...", maKhang, month, period);
            String calcUrl = billingWorkerUrl + "/api/v1/billing/calculate-immediate";
            CalculateImmediateRequest calcReq = new CalculateImmediateRequest(maKhang, month, period, 1, "SO_DEMAND", "CMIS");

            ResponseEntity<String> response = restTemplate.postForEntity(calcUrl, calcReq, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                return billInvoiceRepository.findByMaKhangAndThangChuKyAndKyChot(maKhang, month, period);
            }
        } catch (Exception e) {
            log.error("[FALLBACK] On-demand calculation fallback failed for Account: {}: {}", maKhang, e.getMessage());
        }
        return Optional.empty();
    }
}
