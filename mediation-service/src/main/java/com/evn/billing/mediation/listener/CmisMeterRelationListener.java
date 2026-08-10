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
public class CmisMeterRelationListener {

    @Autowired
    private CmisSyncRepository cmisSyncRepository;

    @Autowired
    private SnapshotEventPublisher snapshotEventPublisher;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "cmis-quan-he-ddo",
            groupId = "mediation-meter-relation-group"
    )
    @Transactional
    public void listenMeterRelation(String message) {
        log.info("[CMIS-SYNC] Received meter relation event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String loaiSuKien = (String) event.get("loai_su_kien");
            String maDdoCha = (String) event.get("ma_ddo_cha");
            String maDdoCon = (String) event.get("ma_ddo_con");
            String maKhang = (String) event.get("ma_khang");

            if (maDdoCha == null || maDdoCha.isEmpty() || maDdoCon == null || maDdoCon.isEmpty() || maKhang == null || maKhang.isEmpty()) {
                log.warn("[CMIS-SYNC] MeterRelation event missing ma_ddo_cha, ma_ddo_con or ma_khang. Skipping.");
                return;
            }

            if ("XOA".equalsIgnoreCase(loaiSuKien)) {
                cmisSyncRepository.deleteMeterRelation(maDdoCha, maDdoCon);
                log.info("[CMIS-SYNC] Deleted meter relation between parent: {} and child: {}", maDdoCha, maDdoCon);
            } else {
                Map<String, Object> duLieu = (Map<String, Object>) event.get("du_lieu");
                if (duLieu != null) {
                    String loaiQuanHe = (String) duLieu.getOrDefault("loai_quan_he", "NETTING");
                    String ngayHieuLuc = (String) duLieu.get("ngay_hieu_luc");
                    String ngayHetHan = (String) duLieu.get("ngay_het_han");

                    cmisSyncRepository.upsertMeterRelation(maDdoCha, maDdoCon, loaiQuanHe, ngayHieuLuc, ngayHetHan);
                    log.info("[CMIS-SYNC] Upserted meter relation: parent = {}, child = {}, type = {}", maDdoCha, maDdoCon, loaiQuanHe);
                }
            }

            // Trigger snapshot refresh
            triggerSnapshotRefresh(maKhang, "R-03", "quan_he_diem_do", "loai_quan_he");

        } catch (Exception e) {
            log.error("[CMIS-SYNC] Error processing meter relation event", e);
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
