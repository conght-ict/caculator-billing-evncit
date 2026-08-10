package com.evn.billing.mediation.listener;

import com.evn.billing.mediation.repository.CmisSyncRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronization;
import com.evn.billing.mediation.service.SnapshotEventPublisher;

import java.util.List;
import java.util.Map;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Slf4j
public class CmisTariffListener {

    @Autowired
    private CmisSyncRepository cmisSyncRepository;

    @Autowired
    private SnapshotEventPublisher snapshotEventPublisher;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "cmis-bieu-gia",
            groupId = "mediation-tariff-group"
    )
    @Transactional
    public void listenTariff(String message) {
        log.info("[CMIS-SYNC] Received tariff event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String maBieuGia = (String) event.get("ma_bieu_gia");

            if (maBieuGia == null || maBieuGia.isEmpty()) {
                log.warn("[CMIS-SYNC] Tariff event missing ma_bieu_gia. Skipping.");
                return;
            }

            Map<String, Object> duLieu = (Map<String, Object>) event.get("du_lieu");
            if (duLieu != null) {
                String tenBieuGia = (String) duLieu.get("ten_bieu_gia");
                String loaiBieuGia = (String) duLieu.get("loai_bieu_gia");
                String ngayHieuLuc = (String) duLieu.get("ngay_hieu_luc");
                if (loaiBieuGia == null || loaiBieuGia.isEmpty() || 
                    ngayHieuLuc == null || ngayHieuLuc.isEmpty()) {
                    throw new IllegalStateException("[CMIS-SYNC] Tariff event missing required fields: loai_bieu_gia or ngay_hieu_luc.");
                }
                String ngayHetHan = (String) duLieu.get("ngay_het_han");
                String quyetDinhPhapLy = (String) duLieu.get("quyet_dinh_phap_ly");
                String trangThai = (String) duLieu.getOrDefault("trang_thai", "ACTIVE");
                List<Map<String, Object>> chiTietGia = (List<Map<String, Object>>) duLieu.get("chi_tiet_gia");
                List<Map<String, Object>> chiTietGiaViet = new ArrayList<>();
                if (chiTietGia != null) {
                    for (Map<String, Object> block : chiTietGia) {
                        Map<String, Object> blockViet = new LinkedHashMap<>();
                        blockViet.put("soThuTu", block.get("step"));
                        blockViet.put("minKwh", block.get("minKwh"));
                        blockViet.put("maxKwh", block.get("maxKwh"));
                        blockViet.put("donGia", block.get("unitPrice"));
                        blockViet.put("tgianBdien", block.get("touPeriod"));
                        chiTietGiaViet.add(blockViet);
                    }
                }
                String chiTietGiaJson = objectMapper.writeValueAsString(chiTietGiaViet);

                // Parse metadata from maBieuGia
                String maNhomnn = "UNKNOWN";
                String khoangDa = "2";
                String maNgiaCmis = "A";
                String thoigianBdien = "KT";
                boolean bacThang = false;
                BigDecimal donGiaPhang = null;

                if (maBieuGia != null && maBieuGia.startsWith("TARIFF_")) {
                    String[] parts = maBieuGia.split("_");
                    if (parts.length > 1) {
                        maNhomnn = parts[1];
                    }
                    for (String p : parts) {
                        if (p.startsWith("CAPDA")) {
                            khoangDa = p.substring(5);
                        }
                    }
                    
                    if (parts.length > 4 && !"CAPDA2".startsWith(parts[2]) && !"CAPDA4".startsWith(parts[2]) && !"CAPDA8".startsWith(parts[2]) && !"CAPDA7".startsWith(parts[2])) {
                        maNgiaCmis = parts[2];
                    }
                    
                    if (maBieuGia.contains("_TT_") || "TT".equals(maNgiaCmis)) {
                        thoigianBdien = "TT";
                    } else if ("STEPPING".equalsIgnoreCase(loaiBieuGia)) {
                        thoigianBdien = "KT";
                        bacThang = true;
                    } else {
                        thoigianBdien = "BT";
                    }

                    if ("FLAT".equalsIgnoreCase(loaiBieuGia) && chiTietGiaJson != null) {
                        try {
                            Matcher m = Pattern.compile("\"donGia\"\\s*:\\s*([\\d\\.]+)").matcher(chiTietGiaJson);
                            if (m.find()) {
                                donGiaPhang = new BigDecimal(m.group(1));
                            }
                        } catch (Exception e) {
                            // Ignore
                        }
                    }
                }

                cmisSyncRepository.upsertTariff(maBieuGia, tenBieuGia, loaiBieuGia, ngayHieuLuc, ngayHetHan, quyetDinhPhapLy, trangThai, chiTietGiaJson,
                                                maNhomnn, khoangDa, maNgiaCmis, thoigianBdien, bacThang, donGiaPhang);
                log.info("[CMIS-SYNC] Synchronized bieu_gia: {}", maBieuGia);

                // Batch trigger snapshot refresh for affected accounts
                triggerBatchSnapshotRefresh(maBieuGia);
            }

        } catch (Exception e) {
            log.error("[CMIS-SYNC] Error processing tariff event", e);
        }
    }

    private void triggerBatchSnapshotRefresh(String maBieuGia) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        executeBatchSnapshotRefresh(maBieuGia);
                    }
                }
            );
        } else {
            executeBatchSnapshotRefresh(maBieuGia);
        }
    }

    private void executeBatchSnapshotRefresh(String maBieuGia) {
        try {
            List<Map<String, Object>> affectedBooks = cmisSyncRepository.findBooksByTariff(maBieuGia);
            if (affectedBooks.isEmpty()) {
                log.info("[CMIS-SYNC] No active books affected by tariff: {}", maBieuGia);
                return;
            }

            log.info("[CMIS-SYNC] Found {} books affected by tariff: {}. Triggering book-level snapshot updates...", affectedBooks.size(), maBieuGia);

            for (Map<String, Object> book : affectedBooks) {
                String dtuongQly = (String) book.get("dtuong_qly");
                String month = (String) book.get("thang_ck");
                int period = ((Number) book.get("ky_chot")).intValue();
                snapshotEventPublisher.publishBookRecreate(dtuongQly, month, period);
            }
        } catch (Exception e) {
            log.error("[CMIS-SYNC] Error executing batch snapshot updates for tariff {}", maBieuGia, e);
        }
    }
}
