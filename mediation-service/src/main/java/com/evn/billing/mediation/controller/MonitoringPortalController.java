package com.evn.billing.mediation.controller;

import com.evn.billing.common.domain.Account;
import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.dto.*;
import com.evn.billing.mediation.service.MonitoringService;
import com.evn.billing.mediation.service.BatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/monitoring")
public class MonitoringPortalController {

    @Autowired
    private MonitoringService monitoringService;

    @Autowired
    private BatchService batchService;

    @Value("${billing.worker.url:http://localhost:8081/worker}")
    private String billingWorkerUrl;

    @Value("${batch.orchestrator.url:http://localhost:8083}")
    private String batchOrchestratorUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    @GetMapping("/accounts")
    public List<Account> getAccounts() {
        return monitoringService.getTopAccounts();
    }

    @GetMapping("/books")
    public ResponseEntity<?> getBooks() {
        return ResponseEntity.ok(monitoringService.getBooks());
    }

    @PostMapping("/accounts-with-status")
    public List<Map<String, Object>> getAccountsWithStatus(@RequestBody BookProgressRequest request) {
        int period = request.getKyChot() != null ? request.getKyChot() : 1;
        return monitoringService.getAccountsWithStatus(request.getDtuongQly(), request.getThangChuKy(), period);
    }

    @GetMapping("/readings")
    public List<MeterUsage> getReadings() {
        return monitoringService.getAllReadings();
    }

    @GetMapping("/snapshots")
    public List<BillingAccountSnapshot> getSnapshots() {
        return monitoringService.getAllSnapshots();
    }

    @GetMapping("/invoices")
    public List<BillInvoice> getInvoices() {
        return monitoringService.getAllInvoices();
    }

    @PostMapping("/logs")
    public ResponseEntity<?> getCalculationLogs(@RequestBody CalculationLogsRequest request) {
        int limit = request.getLimit() != null ? request.getLimit() : 50;
        List<Map<String, Object>> logs = monitoringService.getCalculationLogs(
                request.getDtuongQly(), request.getMaKhang(), request.getTrangThai(), limit);
        return ResponseEntity.ok(logs);
    }

    @GetMapping("/logs/detail/{logId}")
    public ResponseEntity<?> getCalculationLogDetail(@PathVariable String logId) {
        return monitoringService.getCalculationLogDetail(logId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/error-logs")
    public ResponseEntity<?> getErrorLogs(@RequestBody ErrorLogsRequest request) {
        int limit = request.getLimit() != null ? request.getLimit() : 50;
        try {
            List<Map<String, Object>> logs = monitoringService.getErrorLogs(
                    request.getMaKhang(), request.getThangChuKy(), request.getKyChot(), limit);
            return ResponseEntity.ok(logs);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to query error logs: " + e.getMessage());
        }
    }

    @GetMapping("/batch/executions")
    public ResponseEntity<?> getBatchExecutions() {
        try {
            return ResponseEntity.ok(monitoringService.getBatchExecutions());
        } catch (Exception e) {
            return ResponseEntity.ok(java.util.Collections.emptyList());
        }
    }

    @GetMapping("/batch/runs")
    public ResponseEntity<?> getBookBillingRuns() {
        try {
            return ResponseEntity.ok(monitoringService.getBookBillingRuns());
        } catch (Exception e) {
            return ResponseEntity.ok(java.util.Collections.emptyList());
        }
    }

    @GetMapping("/batch/executions/{jobExecutionId}/steps")
    public ResponseEntity<?> getBatchStepExecutions(@PathVariable Long jobExecutionId) {
        try {
            return ResponseEntity.ok(monitoringService.getBatchStepExecutions(jobExecutionId));
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @GetMapping("/batch/runs/{dtuongQly}/{month}/steps")
    public ResponseEntity<?> getBookBillingRunSteps(@PathVariable String dtuongQly, @PathVariable String month) {
        try {
            return ResponseEntity.ok(monitoringService.getBookBillingRunSteps(dtuongQly, month));
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @PostMapping("/batch/run")
    public ResponseEntity<String> runBatchJob(@RequestBody BatchRunRequest request) {
        String dtuongQly = request.getDtuongQly();
        String month = request.getThangChuKy();
        int period = request.getKyChot() != null ? request.getKyChot() : 1;
        long version = request.getPhienBan() != null ? request.getPhienBan() : 1L;

        if (batchService.isBookAlreadyCompleted(dtuongQly, month, period)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Sổ đã được tính cước thành công cho kỳ này. Vui lòng hủy lịch sử tính cước cũ của Sổ trước khi chạy lại.");
        }

        try {
            org.springframework.batch.core.JobExecution execution = batchService.launchBillingJob(dtuongQly, month, period, version);
            return ResponseEntity.ok("Batch job initiated via Mediation Service. Execution ID: " + execution.getId() + ", Status: " + execution.getStatus());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to launch batch job: " + e.getMessage());
        }
    }

    @PostMapping("/billing/cancel")
    public ResponseEntity<String> cancelBilling(@RequestBody CancelBillingRequest request) {
        try {
            return restTemplate.postForEntity(billingWorkerUrl + "/api/v1/billing/cancel", request, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call billing-worker cancel endpoint: " + e.getMessage());
        }
    }

    @PostMapping("/billing/lock")
    public ResponseEntity<String> lockBilling(@RequestBody LockBillingRequest request) {
        try {
            return restTemplate.postForEntity(billingWorkerUrl + "/api/v1/billing/lock", request, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call billing-worker lock endpoint: " + e.getMessage());
        }
    }

    @PostMapping("/billing/book-progress")
    public ResponseEntity<String> getBookProgress(@RequestBody BookProgressRequest request) {
        try {
            return restTemplate.postForEntity(billingWorkerUrl + "/api/v1/billing/book-progress", request, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call billing-worker progress endpoint: " + e.getMessage());
        }
    }

    @PostMapping("/billing/accounts-by-status")
    public ResponseEntity<String> getAccountsByStatus(@RequestBody AccountsByStatusRequest request) {
        try {
            return restTemplate.postForEntity(billingWorkerUrl + "/api/v1/billing/accounts-by-status", request, String.class);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to call billing-worker status list endpoint: " + e.getMessage());
        }
    }

    @PostMapping("/batch/validate")
    public ResponseEntity<String> validateBatchJob(@RequestBody BatchValidateRequest request) {
        String dtuongQly = request.getDtuongQly();
        String month = request.getThangChuKy();
        int period = request.getKyChot() != null ? request.getKyChot() : 1;

        try {
            String result = batchService.validateBatch(dtuongQly, month, period);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to validate batch job: " + e.getMessage());
        }
    }

    @PostMapping("/detail")
    public ResponseEntity<?> getDetail(@RequestBody AccountDetailRequest request) {
        int period = request.getKyChot() != null ? request.getKyChot() : 1;
        Map<String, Object> detail = monitoringService.getAccountDetail(request.getMaKhang(), request.getThangChuKy(), period);
        if (detail == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(detail);
    }

    @PostMapping("/readings/resolve")
    public ResponseEntity<String> resolveReading(@RequestBody ResolveReadingRequest request) {
        try {
            monitoringService.sendReadingResolutionEvent(
                    request.getLoaiXuLy(), request.getMaKhang(), request.getThangChuKy(),
                    request.getDtuongQly(), request.getIdChiSo(), request.getChiSoCuoiDieuChinh());
            return ResponseEntity.ok("Resolution event sent successfully.");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @PostMapping("/billing/operation")
    public ResponseEntity<String> performBillingOperation(@RequestBody BillingOperationRequest request) {
        if ((request.getMaKhang() == null || request.getMaKhang().isEmpty()) 
                && (request.getDtuongQly() == null || request.getDtuongQly().isEmpty())) {
            return ResponseEntity.badRequest().body("Either maKhang or dtuongQly must be provided.");
        }
        try {
            monitoringService.sendBillingOperationEvent(
                    request.getLoaiVanHanh(), request.getMaKhang(), request.getDtuongQly(),
                    request.getThangChuKy(), request.getKyChot());
            return ResponseEntity.ok("Billing operation event sent successfully.");
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }
}
