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

import java.util.*;

@Component
@Slf4j
public class CmisMeterSwapListener {

    @Autowired
    private CmisSyncRepository cmisSyncRepository;

    @Autowired
    private SnapshotEventPublisher snapshotEventPublisher;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "cmis-cong-to",
            groupId = "mediation-meter-swap-group"
    )
    @Transactional
    public void listenMeterSwap(String message) {
        log.info("[CMIS-METER-SWAP] Received meter swap event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String loaiSuKien = (String) event.get("loai_su_kien"); // TREO_CONG_TO | THAO_CONG_TO | THAY_CONG_TO
            String maKhang = (String) event.get("ma_khang");
            String maDdo = (String) event.get("ma_ddo");
            String ngayThay = (String) event.get("ngay_thay");

            if (maDdo == null || maDdo.isEmpty()) {
                log.warn("[CMIS-METER-SWAP] Event is missing ma_ddo. Skipping.");
                return;
            }

            // Retrieve current list from DB using Repository
            List<Map<String, Object>> meterList = new ArrayList<>();
            String currentCto = cmisSyncRepository.findCurrentCto(maDdo);
            
            if (currentCto == null) {
                throw new IllegalStateException("[CMIS-METER-SWAP] Meter point " + maDdo + " not found. Cannot perform meter swap without meter point definition.");
            } else if (!currentCto.trim().isEmpty() && !currentCto.trim().equals("[]") && !currentCto.trim().equals("{}")) {
                try {
                    if (currentCto.trim().startsWith("[")) {
                        meterList = objectMapper.readValue(currentCto, List.class);
                    } else {
                        Map<String, Object> single = objectMapper.readValue(currentCto, Map.class);
                        meterList.add(single);
                    }
                } catch (Exception e) {
                    log.error("[CMIS-METER-SWAP] Error parsing thong_tin_cto JSON: {}", e.getMessage());
                }
            }

            Map<String, Object> oldMeterData = (Map<String, Object>) event.get("cong_to_cu");
            Map<String, Object> newMeterData = (Map<String, Object>) event.get("cong_to_moi");

            // 1. Process old meter decommission
            if (oldMeterData != null) {
                String oldSerial = (String) oldMeterData.get("so_seri");
                Object oldLastIndex = oldMeterData.get("chi_so_thao");
                boolean foundOld = false;
                for (Map<String, Object> cto : meterList) {
                    if (oldSerial != null && oldSerial.equals(cto.get("so_seri"))) {
                        cto.put("ngay_thao", ngayThay != null ? ngayThay : "2025-01-01");
                        cto.put("chi_so_thao", oldLastIndex != null ? oldLastIndex : 0.0);
                        cto.put("trang_thai", "DECOMM");
                        foundOld = true;
                        break;
                    }
                }
                if (!foundOld && oldSerial != null) {
                    Map<String, Object> oldCto = new HashMap<>();
                    oldCto.put("so_seri", oldSerial);
                    oldCto.put("ma_cto", oldSerial);
                    oldCto.put("he_so_nhan", oldMeterData.getOrDefault("he_so_nhan", 1.0));
                    oldCto.put("so_pha", oldMeterData.getOrDefault("so_pha", 1));
                    oldCto.put("danh_sach_bcs", oldMeterData.getOrDefault("danh_sach_bcs", List.of("KT")));
                    oldCto.put("ngay_treo", "2025-01-01");
                    oldCto.put("ngay_thao", ngayThay != null ? ngayThay : "2025-01-01");
                    oldCto.put("chi_so_thao", oldLastIndex != null ? oldLastIndex : 0.0);
                    oldCto.put("trang_thai", "DECOMM");
                    meterList.add(oldCto);
                }
            } else {
                if ("THAY_CONG_TO".equalsIgnoreCase(loaiSuKien) || "THAO_CONG_TO".equalsIgnoreCase(loaiSuKien)) {
                    for (Map<String, Object> cto : meterList) {
                        if ("ACTIVE".equals(cto.get("trang_thai"))) {
                            cto.put("ngay_thao", ngayThay != null ? ngayThay : "2025-01-01");
                            cto.put("trang_thai", "DECOMM");
                        }
                    }
                }
            }

            // 2. Process new meter installation
            if (newMeterData != null) {
                String newSerial = (String) newMeterData.get("so_seri");
                Object newInstallIndex = newMeterData.get("chi_so_treo");
                boolean foundNew = false;
                for (Map<String, Object> cto : meterList) {
                    if (newSerial != null && newSerial.equals(cto.get("so_seri"))) {
                        cto.put("ma_cto", newSerial);
                        cto.put("he_so_nhan", newMeterData.getOrDefault("he_so_nhan", 1.0));
                        cto.put("so_pha", newMeterData.getOrDefault("so_pha", 1));
                        cto.put("danh_sach_bcs", newMeterData.getOrDefault("danh_sach_bcs", List.of("KT")));
                        cto.put("ngay_treo", ngayThay != null ? ngayThay : "2025-01-01");
                        cto.put("chi_so_treo", newInstallIndex != null ? newInstallIndex : 0.0);
                        cto.put("trang_thai", "ACTIVE");
                        foundNew = true;
                        break;
                    }
                }
                if (!foundNew && newSerial != null) {
                    for (Map<String, Object> cto : meterList) {
                        if ("ACTIVE".equals(cto.get("trang_thai"))) {
                            cto.put("trang_thai", "DECOMM");
                            if (cto.get("ngay_thao") == null) {
                                cto.put("ngay_thao", ngayThay != null ? ngayThay : "2025-01-01");
                            }
                        }
                    }

                    Map<String, Object> newCto = new HashMap<>();
                    newCto.put("so_seri", newSerial);
                    newCto.put("ma_cto", newSerial);
                    newCto.put("he_so_nhan", newMeterData.getOrDefault("he_so_nhan", 1.0));
                    newCto.put("so_pha", newMeterData.getOrDefault("so_pha", 1));
                    newCto.put("danh_sach_bcs", newMeterData.getOrDefault("danh_sach_bcs", List.of("KT")));
                    newCto.put("ngay_treo", ngayThay != null ? ngayThay : "2025-01-01");
                    newCto.put("chi_so_treo", newInstallIndex != null ? newInstallIndex : 0.0);
                    newCto.put("trang_thai", "ACTIVE");
                    meterList.add(newCto);
                }
            }

            while (meterList.size() > 10) {
                meterList.remove(0);
            }

            String thongTinCtoStr = objectMapper.writeValueAsString(meterList);

            cmisSyncRepository.updateThongTinCto(maDdo, thongTinCtoStr);
            log.info("[CMIS-METER-SWAP] Successfully synchronized meter swap for DDo: {}, total meters in history: {}", maDdo, meterList.size());

            // 3. Trigger Snapshot rebuild
            if (maKhang != null && !maKhang.isEmpty()) {
                String month = event.containsKey("thang_chu_ky") ? (String) event.get("thang_chu_ky") : null;
                Integer period = event.containsKey("ky_chot") ? ((Number) event.get("ky_chot")).intValue() : null;

                String dtuongQly = cmisSyncRepository.findDtuongQlyByKhang(maKhang);
                if (dtuongQly != null) {
                    final String finalMonth = month;
                    final Integer finalPeriod = period;
                    if (TransactionSynchronizationManager.isActualTransactionActive()) {
                        TransactionSynchronizationManager.registerSynchronization(
                            new TransactionSynchronization() {
                                @Override
                                public void afterCommit() {
                                    executeSnapshotRefresh(maKhang, dtuongQly, finalMonth, finalPeriod);
                                }
                            }
                        );
                    } else {
                        executeSnapshotRefresh(maKhang, dtuongQly, finalMonth, finalPeriod);
                    }
                }
            }

        } catch (Exception e) {
            log.error("[CMIS-METER-SWAP] Error processing meter swap event", e);
        }
    }

    private void executeSnapshotRefresh(String maKhang, String dtuongQly, String month, Integer period) {
        try {
            if (month != null && !month.isEmpty() && period != null) {
                snapshotEventPublisher.publishAccountRecreate(maKhang, dtuongQly, month, period, "R-02", "diem_do", "thong_tin_cto");
            } else {
                List<Map<String, Object>> schedules = cmisSyncRepository.findActiveBookSchedules(dtuongQly);
                for (Map<String, Object> schedule : schedules) {
                    String m = (String) schedule.get("thang_ck");
                    int p = ((Number) schedule.get("ky_chot")).intValue();
                    snapshotEventPublisher.publishAccountRecreate(maKhang, dtuongQly, m, p, "R-02", "diem_do", "thong_tin_cto");
                }
            }
        } catch (Exception e) {
            log.error("[CMIS-METER-SWAP] Failed to trigger snapshot refresh for Account: {}, error: {}", maKhang, e.getMessage());
        }
    }
}
