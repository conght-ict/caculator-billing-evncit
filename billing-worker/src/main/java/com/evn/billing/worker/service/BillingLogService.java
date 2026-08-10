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
import java.sql.Timestamp;

@Service
@Transactional
public class BillingLogService {

    private static final Logger log = LoggerFactory.getLogger(BillingLogService.class);

    @Autowired
    private BillingLogRepository billingLogRepository;

    private final ConcurrentLinkedQueue<CalculationLogEntry> logQueue = new ConcurrentLinkedQueue<>();

    public static class CalculationLogEntry {
        public String idHoaDon;
        public String thangChuKy;
        public String maKhang;
        public String trangThai;
        public String duLieuDauVao;
        public String ketQuaTinhToan;
        public String thongBaoLoi;
        public Long thoiGianXuLyMs;
        public String tenWorker;
        public Timestamp createdAt;

        public CalculationLogEntry(String idHoaDon, String thangChuKy, String maKhang, String trangThai,
                                    String duLieuDauVao, String ketQuaTinhToan, String thongBaoLoi,
                                    Long thoiGianXuLyMs, String tenWorker) {
            this.idHoaDon = idHoaDon;
            this.thangChuKy = thangChuKy;
            this.maKhang = maKhang;
            this.trangThai = trangThai;
            this.duLieuDauVao = duLieuDauVao;
            this.ketQuaTinhToan = ketQuaTinhToan;
            this.thongBaoLoi = thongBaoLoi;
            this.thoiGianXuLyMs = thoiGianXuLyMs;
            this.tenWorker = tenWorker;
            this.createdAt = new Timestamp(System.currentTimeMillis());
        }
    }

    public void enqueueLog(String idHoaDon, String thangChuKy, String maKhang, String trangThai,
                           String duLieuDauVao, String ketQuaTinhToan, String thongBaoLoi,
                           Long thoiGianXuLyMs, String tenWorker) {
        logQueue.offer(new CalculationLogEntry(idHoaDon, thangChuKy, maKhang, trangThai,
                duLieuDauVao, ketQuaTinhToan, thongBaoLoi, thoiGianXuLyMs, tenWorker));
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
