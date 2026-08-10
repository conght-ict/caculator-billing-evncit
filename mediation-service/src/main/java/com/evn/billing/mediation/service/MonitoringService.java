package com.evn.billing.mediation.service;

import com.evn.billing.common.domain.Account;
import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.mediation.dto.ReadingResolutionEvent;
import com.evn.billing.mediation.repository.AccountRepository;
import com.evn.billing.mediation.repository.BillInvoiceRepository;
import com.evn.billing.mediation.repository.BillingAccountSnapshotRepository;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import com.evn.billing.mediation.repository.MonitoringQueryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.util.*;

@Service
public class MonitoringService {

    private static final Logger log = LoggerFactory.getLogger(MonitoringService.class);

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @Autowired
    private BillingAccountSnapshotRepository snapshotRepository;

    @Autowired
    private BillInvoiceRepository billInvoiceRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private MonitoringQueryRepository monitoringQueryRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public List<Account> getTopAccounts() {
        return accountRepository.findTop100ByOrderByMaKhangAsc();
    }

    public List<Map<String, Object>> getBooks() {
        try {
            return monitoringQueryRepository.findBooks();
        } catch (Exception e) {
            log.error("Failed to query books: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public List<Map<String, Object>> getAccountsWithStatus(String dtuongQly, String month, int period) {
        try {
            monitoringQueryRepository.initPendingStatuses(month, period, dtuongQly);
        } catch (Exception e) {
            log.error("Failed to bulk initialize pending statuses: {}", e.getMessage());
        }

        try {
            return monitoringQueryRepository.findAccountsWithStatus(month, period, dtuongQly);
        } catch (Exception e) {
            log.error("Failed to query accounts with status: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public List<MeterUsage> getAllReadings() {
        return meterUsageRepository.findAll();
    }

    public List<BillingAccountSnapshot> getAllSnapshots() {
        return snapshotRepository.findAll();
    }

    public List<BillInvoice> getAllInvoices() {
        return billInvoiceRepository.findAll();
    }

    public List<Map<String, Object>> getCalculationLogs(String dtuongQly, String maKhang, String status, int limit) {
        try {
            return monitoringQueryRepository.findCalculationLogs(dtuongQly, maKhang, status, limit);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    public Optional<Map<String, Object>> getCalculationLogDetail(String logId) {
        return monitoringQueryRepository.findCalculationLogDetail(logId);
    }

    public List<Map<String, Object>> getErrorLogs(String maKhang, String month, Integer period, int limit) {
        return monitoringQueryRepository.findErrorLogs(maKhang, month, period, limit);
    }

    public List<Map<String, Object>> getBatchExecutions() {
        return monitoringQueryRepository.findBatchExecutions();
    }

    public List<Map<String, Object>> getBookBillingRuns() {
        return monitoringQueryRepository.findBookBillingRuns();
    }

    public List<Map<String, Object>> getBatchStepExecutions(Long jobExecutionId) {
        return monitoringQueryRepository.findBatchStepExecutions(jobExecutionId);
    }

    public List<Map<String, Object>> getBookBillingRunSteps(String dtuongQly, String month) {
        List<Long> ids = monitoringQueryRepository.findLatestJobExecutionIds(dtuongQly, month);
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }
        return getBatchStepExecutions(ids.get(0));
    }

    public Map<String, Object> getAccountDetail(String maKhang, String month, int period) {
        Map<String, Object> detail = new HashMap<>();

        Optional<Account> accountOpt = accountRepository.findById(maKhang);
        if (accountOpt.isEmpty()) {
            return null;
        }
        detail.put("account", accountOpt.get());

        List<MeterUsage> usages = meterUsageRepository.findByMaKhangAndThangChuKyAndKyChot(maKhang, month, period);
        detail.put("readings", usages);

        String snapshotId = maKhang + "_" + month + "_p" + period + "_v1";
        Optional<BillingAccountSnapshot> snapshotOpt = snapshotRepository.findById(snapshotId);
        detail.put("snapshot", snapshotOpt.orElse(null));

        Optional<BillInvoice> invoiceOpt = billInvoiceRepository.findByMaKhangAndThangChuKyAndKyChot(maKhang, month, period);
        detail.put("invoice", invoiceOpt.orElse(null));

        return detail;
    }

    public void sendReadingResolutionEvent(String resolutionType, String maKhang, String month, String dtuongQly, Long usageId, BigDecimal correctedEndIndex) {
        ReadingResolutionEvent event = new ReadingResolutionEvent();
        event.setLoaiXuLy(resolutionType);
        event.setMaKhang(maKhang);
        event.setThangChuKy(month);
        event.setDtuongQly(dtuongQly);
        if (usageId != null) {
            event.setIdChiSo(usageId);
        }
        if (correctedEndIndex != null) {
            event.setChiSoCuoiDieuChinh(correctedEndIndex);
        }

        try {
            kafkaTemplate.send("meter-reading-resolutions", maKhang, event);
            log.info("[RESOLVE-API] Sent resolution command: {} for Account: {}", resolutionType, maKhang);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish resolution event: " + e.getMessage(), e);
        }
    }

    public void sendBillingOperationEvent(String operationType, String maKhang, String dtuongQly, String month, Integer period) {
        String finalMonth = month;
        Integer finalPeriod = period;

        // Auto-resolve month and period from active schedule if missing
        if (dtuongQly != null && !dtuongQly.isEmpty()) {
            if (finalMonth == null || finalMonth.isEmpty() || finalPeriod == null) {
                try {
                    Optional<Map<String, Object>> scheduleOpt = monitoringQueryRepository.findLatestActiveScheduleByBook(dtuongQly);
                    if (scheduleOpt.isPresent()) {
                        Map<String, Object> scheduleMap = scheduleOpt.get();
                        if (finalMonth == null || finalMonth.isEmpty()) {
                            finalMonth = (String) scheduleMap.get("thang_ck");
                        }
                        if (finalPeriod == null) {
                            finalPeriod = ((Number) scheduleMap.get("ky_chot")).intValue();
                        }
                    }
                } catch (Exception e) {
                    log.warn("[OPERATIONS-API] Failed to find active book schedule for dtuongQly: {}.", dtuongQly);
                }
            }
        } else if (maKhang != null && !maKhang.isEmpty() && (finalMonth == null || finalMonth.isEmpty() || finalPeriod == null)) {
            try {
                String actualDtuongQly = monitoringQueryRepository.findBookByAccountId(maKhang);
                if (actualDtuongQly != null) {
                    Optional<Map<String, Object>> scheduleOpt = monitoringQueryRepository.findLatestActiveScheduleByBook(actualDtuongQly);
                    if (scheduleOpt.isPresent()) {
                        Map<String, Object> scheduleMap = scheduleOpt.get();
                        if (finalMonth == null || finalMonth.isEmpty()) {
                            finalMonth = (String) scheduleMap.get("thang_ck");
                        }
                        if (finalPeriod == null) {
                            finalPeriod = ((Number) scheduleMap.get("ky_chot")).intValue();
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("[OPERATIONS-API] Failed to resolve month/period for account: {}", maKhang);
            }
        }

        if (finalMonth == null || finalMonth.isEmpty() || finalPeriod == null) {
            throw new IllegalArgumentException(
                    "Cannot resolve billingCycleMonth/period. Provide month and period explicitly or ensure active lich_ghi_chi_so exists.");
        }

        Map<String, Object> event = new HashMap<>();
        event.put("operationType", operationType);
        if (maKhang != null) event.put("maKhang", maKhang);
        if (dtuongQly != null) event.put("dtuongQly", dtuongQly);
        event.put("billingCycleMonth", finalMonth);
        event.put("period", finalPeriod);

        try {
            String partitionKey = maKhang != null ? maKhang : dtuongQly;
            kafkaTemplate.send("billing-operations-topic", partitionKey, event);
            log.info("[OPERATIONS-API] Sent billing operation command: {} for Account: {}, dtuongQly: {}", operationType, maKhang, dtuongQly);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish billing operation event: " + e.getMessage(), e);
        }
    }
}
