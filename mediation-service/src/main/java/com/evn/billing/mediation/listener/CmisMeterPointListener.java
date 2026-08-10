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

@Component
@Slf4j
public class CmisMeterPointListener {

    @Autowired
    private CmisSyncRepository cmisSyncRepository;

    @Autowired
    private SnapshotEventPublisher snapshotEventPublisher;

    private final ObjectMapper objectMapper = new ObjectMapper();


    @KafkaListener(
            topics = "cmis-diem-do",
            groupId = "mediation-meterpoint-group"
    )
    @Transactional
    public void listenMeterPoint(String message) {
        log.info("[CMIS-SYNC] Received meter point event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String loaiSuKien = (String) event.get("loai_su_kien");
            String maDdo = (String) event.get("ma_ddo");
            String maKhang = (String) event.get("ma_khang");

            if (maDdo == null || maDdo.isEmpty() || maKhang == null || maKhang.isEmpty()) {
                log.warn("[CMIS-SYNC] MeterPoint event missing ma_ddo or ma_khang. Skipping.");
                return;
            }

            Map<String, Object> duLieu = (Map<String, Object>) event.get("du_lieu");
            if (duLieu != null) {
                String dtuongQly = (String) duLieu.get("dtuong_qly");
                String maCapda = (String) duLieu.get("ma_capda");
                String maDviqly = (String) duLieu.get("ma_dviqly");

                if (dtuongQly == null || dtuongQly.isEmpty() || 
                    maCapda == null || maCapda.isEmpty() || 
                    maDviqly == null || maDviqly.isEmpty()) {
                    throw new IllegalStateException("[CMIS-SYNC] MeterPoint event missing required fields: dtuong_qly, ma_capda, or ma_dviqly.");
                }

                String trangThai = (String) duLieu.getOrDefault("trang_thai", "ACTIVE");
                int loaiDdo = ((Number) duLieu.getOrDefault("loai_ddo", 1)).intValue();
                int loaiKhang = ((Number) duLieu.getOrDefault("loai_khang", 1)).intValue();
                boolean isDienMt = (Boolean) duLieu.getOrDefault("is_dien_mt", false);

                cmisSyncRepository.upsertMeterPoint(maDdo, maKhang, dtuongQly, maCapda, trangThai, loaiDdo, loaiKhang, isDienMt, maDviqly);
                log.info("[CMIS-SYNC] Synchronized diem_do table for meter point: {}", maDdo);
            }

            // Trigger snapshot refresh
            String ruleId = "TAO_MOI".equals(loaiSuKien) || "XOA".equals(loaiSuKien) ? "R-12" : "R-04";
            triggerSnapshotRefresh(maKhang, ruleId, "diem_do", "trang_thai");

        } catch (Exception e) {
            log.error("[CMIS-SYNC] Error processing meter point event", e);
        }
    }

    private void triggerSnapshotRefresh(String maKhang, String ruleId, String bangNguon, String truongThayDoi) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        executeSnapshotRefresh(maKhang, ruleId, bangNguon, truongThayDoi);
                    }
                }
            );
        } else {
            executeSnapshotRefresh(maKhang, ruleId, bangNguon, truongThayDoi);
        }
    }

    private void executeSnapshotRefresh(String maKhang, String ruleId, String bangNguon, String truongThayDoi) {
        try {
            String dtuongQly = cmisSyncRepository.findDtuongQlyByKhang(maKhang);
            if (dtuongQly == null) {
                log.warn("[CMIS-SYNC] No management object found for account: {}. Skipping snapshot refresh.", maKhang);
                return;
            }

            List<Map<String, Object>> schedules = cmisSyncRepository.findActiveBookSchedules(dtuongQly);
            if (schedules.isEmpty()) {
                log.warn("[CMIS-SYNC] No active book schedules found for object: {}. Skipping snapshot refresh.", dtuongQly);
                return;
            }

            for (Map<String, Object> schedule : schedules) {
                String month = (String) schedule.get("thang_ck");
                int period = ((Number) schedule.get("ky_chot")).intValue();
                snapshotEventPublisher.publishAccountRecreate(maKhang, dtuongQly, month, period, ruleId, bangNguon, truongThayDoi);
            }
        } catch (Exception e) {
            log.error("[CMIS-SYNC] Failed to trigger snapshot refresh for Account: {}, error: {}", maKhang, e.getMessage());
        }
    }
}
