package com.evn.billing.worker.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import com.evn.billing.worker.repository.BillingLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
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
    private BillingLogRepository billingLogRepository;

    private final ConcurrentLinkedQueue<CalculationLogEntry> logQueue = new ConcurrentLinkedQueue<>();

    public static class CalculationLogEntry {
        public UUID logId;
        public String dtuongQly;
        public String maKhang;
        public String billingCycleMonth;
        public int period;
        public String status;
        public String inputData;
        public String outputData;
        public String errorMessage;
        public java.sql.Timestamp createdAt;

        public CalculationLogEntry(String dtuongQly, String maKhang, String billingCycleMonth, int period, String status, String inputData, String outputData, String errorMessage) {
            this.logId = UUID.randomUUID();
            this.dtuongQly = dtuongQly;
            this.maKhang = maKhang;
            this.billingCycleMonth = billingCycleMonth;
            this.period = period;
            this.status = status;
            this.inputData = inputData;
            this.outputData = outputData;
            this.errorMessage = errorMessage;
            this.createdAt = new java.sql.Timestamp(System.currentTimeMillis());
        }
    }

    public void enqueueLog(String dtuongQly, String maKhang, String billingCycleMonth, int period, String status, String inputData, String outputData, String errorMessage) {
        logQueue.offer(new CalculationLogEntry(dtuongQly, maKhang, billingCycleMonth, period, status, inputData, outputData, errorMessage));
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
            billingLogRepository.batchInsertCalculationLogs(entries);
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
