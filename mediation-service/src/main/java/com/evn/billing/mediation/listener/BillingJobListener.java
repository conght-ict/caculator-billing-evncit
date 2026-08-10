package com.evn.billing.mediation.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import com.evn.billing.mediation.repository.BillingRunRepository;

public class BillingJobListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(BillingJobListener.class);

    @Autowired
    private BillingRunRepository billingRunRepository;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        String dtuongQly = jobExecution.getJobParameters().getString("dtuongQly");
        String month = jobExecution.getJobParameters().getString("month");
        Long periodParam = jobExecution.getJobParameters().getLong("period");
        int period = periodParam != null ? periodParam.intValue() : 1;

        log.info("BeforeJob Hook: Validating snapshot readiness for Book: {}, Month: {}, Period: {}", dtuongQly, month, period);
        boolean snapshotGenerated = billingRunRepository.isSnapshotGenerated(dtuongQly, month, period);
        if (!snapshotGenerated) {
            log.warn("BeforeJob Hook: Snapshot not pre-generated for book: {}! Worker will fallback to DB configuration lookup.", dtuongQly);
        } else {
            log.info("BeforeJob Hook: Snapshot is verified pre-generated and cache is warm.");
        }

        // Count total accounts in this Book
        int totalAccounts = 0;
        try {
            totalAccounts = billingRunRepository.countActiveAccountsInBook(dtuongQly);
        } catch (Exception e) {
            log.error("Failed to count accounts for book: {}", dtuongQly, e);
        }

        // Transition SUCCESS to SUCCESS_CMIS for this book/month/period
        int alreadyCalculated = 0;
        try {
            alreadyCalculated = billingRunRepository.countSuccessAccounts(dtuongQly, month, period);
            billingRunRepository.transitionSuccessToSuccessCmis(dtuongQly, month, period);
            log.info("Transitioned {} accounts from SUCCESS to SUCCESS_CMIS for Book: {}, Month: {}, Period: {}", alreadyCalculated, dtuongQly, month, period);
        } catch (Exception e) {
            log.error("Failed to transition SUCCESS to SUCCESS_CMIS", e);
        }

        // Upsert lich_ghi_dqly progress
        try {
            String maDviqly = billingRunRepository.findMaDviqlyByBook(dtuongQly);
            billingRunRepository.upsertBookRunProcessing(dtuongQly, month, period, totalAccounts, alreadyCalculated, maDviqly);
            log.info("BeforeJob Hook: lich_ghi_dqly record initialized for Book: {} with total accounts: {}, already calculated: {}.", dtuongQly, totalAccounts, alreadyCalculated);

            // Transition all eligible accounts in the Book to PROCESSING to allow worker claim
            billingRunRepository.transitionEligibleAccountsToProcessing(dtuongQly, month, period);
            log.info("BeforeJob Hook: Transitioned eligible accounts in Book: {} to PROCESSING state.", dtuongQly);
        } catch (Exception e) {
            log.error("Failed to upsert lich_ghi_dqly or transition accounts to PROCESSING for book: {}", dtuongQly, e);
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String dtuongQly = jobExecution.getJobParameters().getString("dtuongQly");
        String month = jobExecution.getJobParameters().getString("month");
        Long periodParam = jobExecution.getJobParameters().getLong("period");
        int period = periodParam != null ? periodParam.intValue() : 1;
        String finalStatus = jobExecution.getStatus().toString();

        log.info("AfterJob Hook: Dispatching batch job execution complete. Status: {}", finalStatus);

        try {
            // Fix: Dùng DISPATCHED thay vì COMPLETED
            // COMPLETED chỉ được đặt bởi billing-worker khi TẤT CẢ khách hàng đã xử lý xong
            String scheduleRunStatus = "DISPATCHED";
            if ("FAILED".equals(finalStatus) || "STOPPED".equals(finalStatus)) {
                scheduleRunStatus = "FAILED";
            }
            billingRunRepository.updateBookRunFinalStatus(dtuongQly, month, period, scheduleRunStatus);
            log.info("AfterJob Hook: lich_ghi_dqly tthai_chay updated to {} for Book: {}, Month: {}, Period: {}.", scheduleRunStatus, dtuongQly, month, period);
        } catch (Exception e) {
            log.error("Failed to update final status of lich_ghi_dqly for book: {}", dtuongQly, e);
        }
    }
}
