package com.evn.billing.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;
import java.util.UUID;

public class BillingJobListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(BillingJobListener.class);
    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    /**
     * Triggers configuration freezing snapshot building and cache synchronization
     * on the snapshot-generator module prior to starting batch reading.
     */
    @Override
    public void beforeJob(JobExecution jobExecution) {
        String bookId = jobExecution.getJobParameters().getString("bookId");
        String month = jobExecution.getJobParameters().getString("month");

        log.info("BeforeJob Hook: Triggering snapshot building for Book: {}, Cycle: {}", bookId, month);
        
        // Rest API call to snapshot-generator microservice
        String url = "http://localhost:8082/api/v1/snapshots/generate?bookId=" + bookId + "&month=" + month;
        try {
            restTemplate.postForEntity(url, null, String.class);
            log.info("BeforeJob Hook: Snapshot profile freeze and Redis warmup completed successfully.");
        } catch (Exception e) {
            log.error("BeforeJob Hook Warning: Snapshot service trigger failed: {}", e.getMessage());
        }

        // Count total accounts in this Book
        int totalAccounts = 0;
        try {
            totalAccounts = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM account WHERE book_id = ?", Integer.class, bookId);
        } catch (Exception e) {
            log.error("Failed to count accounts for book: {}", bookId, e);
        }

        // Upsert book_billing_run status
        try {
            UUID runId = UUID.randomUUID();
            String sql = "INSERT INTO book_billing_run (run_id, book_id, billing_cycle_month, status, total_accounts, processed_accounts, success_accounts, failed_accounts, created_at, updated_at) " +
                    "VALUES (?, ?, ?, 'PROCESSING', ?, 0, 0, 0, NOW(), NOW()) " +
                    "ON CONFLICT (book_id, billing_cycle_month) DO UPDATE SET " +
                    "status = 'PROCESSING', total_accounts = EXCLUDED.total_accounts, processed_accounts = 0, success_accounts = 0, failed_accounts = 0, updated_at = NOW()";
            jdbcTemplate.update(sql, runId, bookId, month, totalAccounts);
            log.info("BeforeJob Hook: book_billing_run record initialized for Book: {} with {} accounts.", bookId, totalAccounts);
        } catch (Exception e) {
            log.error("Failed to upsert book_billing_run for book: {}", bookId, e);
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String bookId = jobExecution.getJobParameters().getString("bookId");
        String month = jobExecution.getJobParameters().getString("month");
        String finalStatus = jobExecution.getStatus().toString();

        log.info("AfterJob Hook: Dispatching batch job execution complete. Status: {}", finalStatus);

        try {
            String sql = "UPDATE book_billing_run SET status = ?, updated_at = NOW() WHERE book_id = ? AND billing_cycle_month = ?";
            jdbcTemplate.update(sql, finalStatus, bookId, month);
            log.info("AfterJob Hook: book_billing_run status updated to {} for Book: {}, Month: {}.", finalStatus, bookId, month);
        } catch (Exception e) {
            log.error("Failed to update final status of book_billing_run for book: {}", bookId, e);
        }
    }
}

