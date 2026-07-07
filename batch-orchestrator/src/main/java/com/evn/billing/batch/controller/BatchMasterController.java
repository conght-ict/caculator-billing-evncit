package com.evn.billing.batch.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import com.evn.billing.batch.service.BatchService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/batch")
public class BatchMasterController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job billingJob;

    @Autowired
    private BatchService batchService;

    @Autowired
    private com.evn.billing.batch.repository.BookBillingScheduleRepository bookBillingScheduleRepository;

    @GetMapping("/validate")
    public ResponseEntity<String> validateBatch(
            @RequestParam String bookId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Integer period) {
        java.util.Optional<com.evn.billing.common.domain.BookBillingSchedule> runOpt = 
                bookBillingScheduleRepository.findByBookIdAndBillingCycleMonthAndPeriod(bookId, month, period);
        if (runOpt.isPresent() && "COMPLETED".equals(runOpt.get().getRunStatus())) {
            return ResponseEntity.ok("BOOK_ALREADY_CALCULATED_NEEDS_CANCEL");
        }

        boolean isReady = batchService.validateBookReadiness(bookId, month, period);
        if (isReady) {
            return ResponseEntity.ok("READY_FOR_BILLING");
        } else {
            return ResponseEntity.ok("READY_CHECK_FAILED_PENDING_MANUAL_READINGS");
        }
    }

    @PostMapping("/run")
    public ResponseEntity<String> runBatch(
            @RequestParam String bookId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Integer period,
            @RequestParam(defaultValue = "1") Long version) {
        
        java.util.Optional<com.evn.billing.common.domain.BookBillingSchedule> runOpt = 
                bookBillingScheduleRepository.findByBookIdAndBillingCycleMonthAndPeriod(bookId, month, period);
        if (runOpt.isPresent() && "COMPLETED".equals(runOpt.get().getRunStatus())) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Sổ đã được tính cước thành công cho kỳ này. Vui lòng hủy lịch sử tính cước cũ của Sổ trước khi chạy lại.");
        }
        
        try {
            Map<String, JobParameter<?>> params = new HashMap<>();
            params.put("bookId", new JobParameter<>(bookId, String.class));
            params.put("month", new JobParameter<>(month, String.class));
            params.put("period", new JobParameter<>(period.longValue(), Long.class));
            params.put("calculationVersion", new JobParameter<>(version, Long.class));
            params.put("time", new JobParameter<>(System.currentTimeMillis(), Long.class)); // avoids job parameter collision

            JobParameters jobParameters = new JobParameters(params);
            
            JobExecution execution = jobLauncher.run(billingJob, jobParameters);
            
            return ResponseEntity.ok("Batch job initiated. Execution ID: " + execution.getId() + ", Status: " + execution.getStatus());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to launch batch job: " + e.getMessage());
        }
    }
}
