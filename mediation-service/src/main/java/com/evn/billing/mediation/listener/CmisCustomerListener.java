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
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Component
@Slf4j
public class CmisCustomerListener {

    @Autowired
    private CmisSyncRepository cmisSyncRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();

    @KafkaListener(
            topics = "cmis-khach-hang",
            groupId = "mediation-customer-group"
    )
    @Transactional
    public void listenCustomer(String message) {
        log.info("[CMIS-SYNC] Received customer event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String maKhang = (String) event.get("ma_khang");
            
            if (maKhang == null || maKhang.isEmpty()) {
                log.warn("[CMIS-SYNC] Customer event missing ma_khang. Skipping.");
                return;
            }

            Map<String, Object> duLieu = (Map<String, Object>) event.get("du_lieu");
            if (duLieu != null) {
                String tenKhang = (String) duLieu.get("ten_khang");
                String diaChi = (String) duLieu.get("dia_chi");
                String dienThoai = (String) duLieu.get("dien_thoai");
                String email = (String) duLieu.get("email");
                String maSoThue = (String) duLieu.get("ma_so_thue");
                String trangThai = (String) duLieu.getOrDefault("trang_thai", "ACTIVE");
                String maDviqly = (String) duLieu.get("ma_dviqly");
                if (maDviqly == null || maDviqly.isEmpty()) {
                    throw new IllegalStateException("[CMIS-SYNC] Customer event missing required field: ma_dviqly.");
                }

                cmisSyncRepository.upsertCustomer(maKhang, tenKhang, trangThai, diaChi, dienThoai, email, maSoThue, maDviqly);
                log.info("[CMIS-SYNC] Synchronized khach_hang for account: {}", maKhang);
            }

            // Trigger snapshot refresh
            triggerSnapshotRefresh(maKhang, "R-11", "khach_hang", "trang_thai");

        } catch (Exception e) {
            log.error("[CMIS-SYNC] Error processing customer event", e);
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

            String month = null;
            Integer period = null;
            Map<String, Object> schedule = cmisSyncRepository.findActiveBookSchedule(dtuongQly);
            if (schedule != null) {
                month = (String) schedule.get("thang_ck");
                period = ((Number) schedule.get("ky_chot")).intValue();
            } else {
                log.warn("[CMIS-SYNC] No active book schedule found for object: {}. Skipping snapshot refresh.", dtuongQly);
                return;
            }

            String snapshotGenUrl = "http://localhost:8082/api/v1/snapshots/generate-for-account?maKhang=" + maKhang 
                    + "&month=" + month + "&period=" + period + "&ruleId=" + ruleId + "&bangNguon=" + bangNguon 
                    + "&truongThayDoi=" + truongThayDoi;
            
            restTemplate.postForEntity(snapshotGenUrl, null, String.class);
            log.info("[CMIS-SYNC] Triggered snapshot update for Account: {}", maKhang);
        } catch (Exception e) {
            log.error("[CMIS-SYNC] Failed to trigger snapshot refresh for Account: {}, error: {}", maKhang, e.getMessage());
        }
    }
}
