package com.evn.billing.mediation.controller;

import com.evn.billing.common.dto.BatchValidateRequest;
import com.evn.billing.common.dto.BatchRunRequest;
import com.evn.billing.mediation.service.BatchService;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/batch")
public class BatchMasterController {

    @Autowired
    private BatchService batchService;

    @PostMapping("/validate")
    public ResponseEntity<String> validateBatch(@RequestBody BatchValidateRequest request) {
        String dtuongQly = request.getDtuongQly();
        String month = request.getThangChuKy();
        int period = request.getKyChot() != null ? request.getKyChot() : 1;

        String result = batchService.validateBatch(dtuongQly, month, period);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/run")
    public ResponseEntity<String> runBatch(@RequestBody BatchRunRequest request) {
        String dtuongQly = request.getDtuongQly();
        String month = request.getThangChuKy();
        int period = request.getKyChot() != null ? request.getKyChot() : 1;
        long version = request.getPhienBan() != null ? request.getPhienBan() : 1L;

        if (batchService.isBookAlreadyCompleted(dtuongQly, month, period)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Sổ đã được tính cước thành công cho kỳ này. Vui lòng hủy lịch sử tính cước cũ của Sổ trước khi chạy lại.");
        }

        try {
            JobExecution execution = batchService.launchBillingJob(dtuongQly, month, period, version);
            return ResponseEntity.ok("Batch job initiated via Mediation Service. Execution ID: " + execution.getId() + ", Status: " + execution.getStatus());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to launch batch job: " + e.getMessage());
        }
    }
}
