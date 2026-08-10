package com.evn.billing.worker.service;

import com.evn.billing.common.dto.BillingTaskDto;
import com.evn.billing.worker.repository.SelfHealingRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class SelfHealingService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private SelfHealingRepository selfHealingRepository;
 
    @Autowired
    private com.evn.billing.worker.repository.BillInvoiceRepository billInvoiceRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final String RETRY_TOPIC = "billing-retry-topic";
    private static final String EXECUTION_TOPIC = "billing-execution-topic";

    /**
     * Handles task execution failure. Route to Retry Queue or DLQ based on attempts count.
     */
    public void handleFailure(BillingTaskDto task, String errorMessage) {
        log.warn("[SELF-HEALING] Handling failure for Account: {}, Error: {}", task.getMaKhang(), errorMessage);

        // 1. Log error to nhat_ky_loi_tinh_toan
        logErrorToDb(task.getMaKhang(), task.getThangChuKy(), task.getKyChot(), "CALCULATION_ERROR", errorMessage);

        int attempts = task.getPhienBanTinh(); // We can use calculationVersion to track retry attempts in task DTO

        if (attempts < MAX_RETRY_ATTEMPTS) {
            // Route to Retry Queue với notBefore header để tránh Thread.sleep trên consumer
            task.setPhienBanTinh(attempts + 1);
            long delayMs = getDelayForAttempt(attempts + 1);
            long notBefore = System.currentTimeMillis() + delayMs;
            try {
                ProducerRecord<String, Object> retryRecord =
                        new ProducerRecord<>(RETRY_TOPIC, task.getMaKhang(), task);
                retryRecord.headers().add("notBefore",
                        java.nio.ByteBuffer.allocate(8).putLong(notBefore).array());
                kafkaTemplate.send(retryRecord);
                log.info("[SELF-HEALING] Dispatched task to Retry Queue (Attempt {}/{}, notBefore={}s) for Account: {}",
                        attempts + 1, MAX_RETRY_ATTEMPTS, delayMs / 1000, task.getMaKhang());
            } catch (Exception e) {
                log.error("[SELF-HEALING] Failed to route to retry topic: {}", e.getMessage());
            }
        } else {
            // Move to DLQ
            logToDqlDb(task, errorMessage);
        }
    }

    private void logErrorToDb(String maKhang, String month, int period, String errorType, String errorDetails) {
        try {
            selfHealingRepository.insertErrorLog(maKhang, month, period, errorType, errorDetails);
        } catch (Exception e) {
            log.error("[SELF-HEALING] Failed to log error to DB: {}", e.getMessage());
        }
    }

    private void logToDqlDb(BillingTaskDto task, String errorMessage) {
        log.error("[DLQ-ALERT] Task for Account: {} failed {} times. Moving to DLQ.", task.getMaKhang(), MAX_RETRY_ATTEMPTS);
        
        // Calculate next retry (e.g. after 30 minutes for DLQ items)
        LocalDateTime nextRetry = LocalDateTime.now().plusMinutes(30);

        try {
            selfHealingRepository.insertDlqTask(
                    task.getMaKhang(),
                    task.getThangChuKy(),
                    task.getKyChot(),
                    MAX_RETRY_ATTEMPTS,
                    errorMessage,
                    Timestamp.valueOf(nextRetry));
        } catch (Exception e) {
            log.error("[SELF-HEALING] Failed to write task to DLQ DB: {}", e.getMessage());
        }
    }

    /**
     * Cron Job sweeps DLQ table (lich_xu_ly_lai) every 1 minute.
     * Regenerates and retries tasks that are due.
     */
    @Scheduled(fixedDelay = 60000)
    public void processScheduledRetries() {
        log.debug("[SELF-HEALING] Sweeping DLQ table for scheduled retries...");
        try {
            List<Map<String, Object>> pendingTasks = selfHealingRepository.findPendingRetryTasks(100);
            if (pendingTasks.isEmpty()) return;

            log.info("[SELF-HEALING] Found {} pending DLQ tasks to retry.", pendingTasks.size());

            for (Map<String, Object> t : pendingTasks) {
                Long taskId = ((Number) t.get("id_nhiem_vu")).longValue();
                String maKhang = (String) t.get("ma_khang");
                String month = (String) t.get("thang_chu_ky");
                int period = ((Number) t.get("ky_chot")).intValue();
                int retries = ((Number) t.get("so_lan_thu_lai")).intValue();

                // Check if snapshot is malformed -> regenerate if possible
                // We mock regeneration by updating status and dispatching a new BillingTaskDto
                log.info("[SELF-HEALING] Re-triggering execution for Account: {}, Month: {}, Period: {}", maKhang, month, period);

                // Update task status to COMPLETED in DLQ
                selfHealingRepository.markRetryTaskCompleted(taskId);

                String dtuongQly = null;
                try {
                    dtuongQly = selfHealingRepository.findBookFromBillingStatus(maKhang, month, period);
                } catch (Exception ex) {
                    try {
                        dtuongQly = selfHealingRepository.findBookByAccountId(maKhang);
                    } catch (Exception ex2) {
                        // ignore
                    }
                }

                if (dtuongQly == null) {
                    log.error("[SELF-HEALING] Cannot resolve dtuongQly (dtuong_qly) for account: {}. Skipping self-healing retry.", maKhang);
                    selfHealingRepository.markRetryTaskFailed(taskId, "Missing dtuongQly");
                    continue;
                }

                // Dispatch task back to Kafka execution queue
                BillingTaskDto taskDto = new BillingTaskDto();
                taskDto.setMaKhang(maKhang);
                taskDto.setDtuongQly(dtuongQly);
                taskDto.setThangChuKy(month);
                taskDto.setKyChot(period);
                int nextVersion = (int) billInvoiceRepository.countByMaKhangAndThangChuKyAndKyChot(maKhang, month, period) + 1;
                taskDto.setPhienBanTinh(nextVersion);
                taskDto.setTriggeredBy("SELF_HEALING");
                taskDto.setTraceId(java.util.UUID.randomUUID().toString().replace("-", ""));
                taskDto.setDanhSachChiSo(new ArrayList<>()); // Readings will be re-fetched by worker from DB

                kafkaTemplate.send(EXECUTION_TOPIC, maKhang, taskDto);
            }
        } catch (Exception e) {
            log.error("[SELF-HEALING] Error sweeping DLQ table: {}", e.getMessage(), e);
        }
    }

    private long getDelayForAttempt(int attempt) {
        switch (attempt) {
            case 1: return 30_000L;   // 30 giây
            case 2: return 120_000L;  // 2 phút
            case 3: return 300_000L;  // 5 phút (KHÔNG VƯỢT max.poll.interval.ms=300000)
            default: return 30_000L;
        }
    }
}
