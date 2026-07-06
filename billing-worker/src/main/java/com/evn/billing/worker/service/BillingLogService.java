package com.evn.billing.worker.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class BillingLogService {

    private static final Logger log = LoggerFactory.getLogger(BillingLogService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ConcurrentLinkedQueue<CalculationLogEntry> logQueue = new ConcurrentLinkedQueue<>();

    public static class CalculationLogEntry {
        public UUID logId;
        public String bookId;
        public String accountId;
        public String billingCycleMonth;
        public String status;
        public String inputData;
        public String outputData;
        public String errorMessage;
        public java.sql.Timestamp createdAt;

        public CalculationLogEntry(String bookId, String accountId, String billingCycleMonth, String status, String inputData, String outputData, String errorMessage) {
            this.logId = UUID.randomUUID();
            this.bookId = bookId;
            this.accountId = accountId;
            this.billingCycleMonth = billingCycleMonth;
            this.status = status;
            this.inputData = inputData;
            this.outputData = outputData;
            this.errorMessage = errorMessage;
            this.createdAt = new java.sql.Timestamp(System.currentTimeMillis());
        }
    }

    @PostConstruct
    public void initSchema() {
        try {
            log.info("Initializing billing_calculation_log database table...");
            jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS billing_calculation_log (" +
                    "log_id UUID PRIMARY KEY, " +
                    "book_id VARCHAR(50) NOT NULL, " +
                    "account_id VARCHAR(50) NOT NULL, " +
                    "billing_cycle_month VARCHAR(20) NOT NULL, " +
                    "status VARCHAR(20) NOT NULL, " +
                    "input_data JSONB, " +
                    "output_data JSONB, " +
                    "error_message TEXT, " +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_calc_log_book ON billing_calculation_log(book_id)");
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_calc_log_account ON billing_calculation_log(account_id)");
            log.info("Table billing_calculation_log and indexes initialized successfully.");
        } catch (Exception e) {
            log.error("Failed to initialize billing_calculation_log schema: {}", e.getMessage(), e);
        }
    }

    public void enqueueLog(String bookId, String accountId, String billingCycleMonth, String status, String inputData, String outputData, String errorMessage) {
        logQueue.offer(new CalculationLogEntry(bookId, accountId, billingCycleMonth, status, inputData, outputData, errorMessage));
    }

    @Scheduled(fixedDelay = 200)
    public void flushLogs() {
        if (logQueue.isEmpty()) return;

        List<CalculationLogEntry> entries = new ArrayList<>();
        CalculationLogEntry entry;
        while ((entry = logQueue.poll()) != null) {
            entries.add(entry);
            if (entries.size() >= 1000) {
                break;
            }
        }

        if (entries.isEmpty()) return;

        try {
            String sql = "INSERT INTO billing_calculation_log (log_id, book_id, account_id, billing_cycle_month, status, input_data, output_data, error_message, created_at) " +
                    "VALUES (?::uuid, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)";
            
            List<Object[]> batchArgs = new ArrayList<>();
            for (CalculationLogEntry e : entries) {
                batchArgs.add(new Object[] {
                    e.logId.toString(),
                    e.bookId,
                    e.accountId,
                    e.billingCycleMonth,
                    e.status,
                    e.inputData,
                    e.outputData,
                    e.errorMessage,
                    e.createdAt
                });
            }

            int[] argTypes = new int[] {
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.TIMESTAMP
            };

            jdbcTemplate.batchUpdate(sql, batchArgs, argTypes);
        } catch (Exception ex) {
            log.error("Failed to save calculation logs batch: {}", ex.getMessage(), ex);
        }
    }

    @PreDestroy
    public void flushRemainingLogs() {
        log.info("Shutting down BillingLogService. Flushing remaining logs...");
        flushLogs();
    }
}
