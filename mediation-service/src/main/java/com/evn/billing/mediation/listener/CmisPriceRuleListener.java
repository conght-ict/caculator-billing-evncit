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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class CmisPriceRuleListener {

    @Autowired
    private CmisSyncRepository cmisSyncRepository;

    @Autowired
    private SnapshotEventPublisher snapshotEventPublisher;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "cmis-ap-gia",
            groupId = "mediation-pricerule-group"
    )
    @Transactional
    public void listenPriceRule(String message) {
        log.info("[CMIS-SYNC] Received price rule event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String maDdo = (String) event.get("ma_ddo");
            String maKhang = (String) event.get("ma_khang");

            if (maDdo == null || maDdo.isEmpty() || maKhang == null || maKhang.isEmpty()) {
                log.warn("[CMIS-SYNC] PriceRule event missing ma_ddo or ma_khang. Skipping.");
                return;
            }

            List<Map<String, Object>> inputRules = (List<Map<String, Object>>) event.get("danh_sach_ap_gia");
            List<Map<String, Object>> dbRules = new ArrayList<>();

            if (inputRules != null) {
                for (Map<String, Object> r : inputRules) {
                    Map<String, Object> dbRule = new HashMap<>();
                    dbRule.put("soThuTu", r.get("so_thu_tu"));
                    dbRule.put("maNhomnn", r.get("ma_nhomnn"));
                    dbRule.put("maNn", r.get("ma_nn"));
                    dbRule.put("maCapda", r.get("ma_capda"));
                    dbRule.put("maNgia", r.get("ma_ngia"));
                    dbRule.put("tgianBdien", r.get("tgian_bdien"));
                    dbRule.put("dinhMuc", r.get("dinh_muc"));
                    dbRule.put("loaiDmuc", r.get("loai_dmuc"));
                    dbRule.put("soHo", r.getOrDefault("so_ho", 1));
                    dbRules.add(dbRule);
                }
            }

            String newRulesJson = objectMapper.writeValueAsString(dbRules);

            cmisSyncRepository.updatePriceRules(maDdo, maKhang, newRulesJson);
            log.info("[CMIS-SYNC] Replaced danh_sach_ap_gia for meter point: {} (Rules count: {})", maDdo, dbRules.size());

            // Trigger snapshot refresh
            triggerSnapshotRefresh(maKhang, "R-01", "diem_do", "danh_sach_ap_gia");

        } catch (Exception e) {
            log.error("[CMIS-SYNC] Error processing price rule event", e);
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
