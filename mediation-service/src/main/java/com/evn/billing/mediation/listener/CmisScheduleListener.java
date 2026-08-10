package com.evn.billing.mediation.listener;

import com.evn.billing.mediation.repository.CmisSyncRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class CmisScheduleListener {

    @Autowired
    private CmisSyncRepository cmisSyncRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "cmis-lich-ghi",
            groupId = "cmis-schedule-group"
    )
    @Transactional
    public void listenCmisSchedule(String message) {
        log.info("[SCHEDULE-KAFKA] Received schedule sync event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String loaiSuKien = (String) event.get("loai_su_kien");

            if ("RETURN_TO_CORRECTION".equalsIgnoreCase(loaiSuKien) || "RETURNED_TO_CORRECTION".equalsIgnoreCase(loaiSuKien)) {
                String maKhang = (String) event.get("ma_khang");
                String dtuongQly = (String) event.get("dtuong_qly");

                if ((dtuongQly == null || dtuongQly.isEmpty()) && maKhang != null && !maKhang.isEmpty()) {
                    dtuongQly = cmisSyncRepository.findDtuongQlyByKhang(maKhang);
                }
                if (dtuongQly == null || dtuongQly.isEmpty()) {
                    log.warn("[SCHEDULE-KAFKA] Missing dtuong_qly for RETURN_TO_CORRECTION event. Skipping.");
                    return;
                }

                // Query active month and period from Repository / DB
                String month = (String) event.get("thang_chu_ky");
                Integer period = event.containsKey("ky_chot") && event.get("ky_chot") != null
                        ? ((Number) event.get("ky_chot")).intValue()
                        : null;
                try {
                    if (month == null || month.isEmpty() || period == null) {
                        Map<String, Object> scheduleMap = cmisSyncRepository.findActiveBookSchedule(dtuongQly);
                        if (scheduleMap != null) {
                            if (month == null || month.isEmpty()) {
                                month = (String) scheduleMap.get("thang_ck");
                            }
                            if (period == null) {
                                period = ((Number) scheduleMap.get("ky_chot")).intValue();
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn("[SCHEDULE-KAFKA] Failed to query active schedule for return of dtuong_qly: {}.", dtuongQly);
                }

                if (month == null || month.isEmpty() || period == null) {
                    log.warn("[SCHEDULE-KAFKA] Cannot resolve month/period for RETURN_TO_CORRECTION event. Skipping.");
                    return;
                }

                // Route to billing-worker via operations topic
                Map<String, Object> cancelEvent = new HashMap<>();
                cancelEvent.put("operationType", "CANCEL_BILLING");
                cancelEvent.put("maKhang", maKhang);
                cancelEvent.put("billingCycleMonth", month);
                cancelEvent.put("period", period);

                kafkaTemplate.send("billing-operations-topic", maKhang, cancelEvent);
                log.info("[SCHEDULE-KAFKA] Dispatched CANCEL_BILLING operation for account: {} returned to correction.", maKhang);
                return;
            }

            Map<String, Object> duLieu = (Map<String, Object>) event.get("du_lieu");
            if (duLieu == null) {
                log.warn("[SCHEDULE-KAFKA] Event missing du_lieu. Skipping.");
                return;
            }

            String month = (String) duLieu.get("thang_ck");
            Integer periodVal = duLieu.containsKey("ky_chot") && duLieu.get("ky_chot") != null
                    ? ((Number) duLieu.get("ky_chot")).intValue()
                    : null;
            int period = periodVal != null ? periodVal : 1;

            String fromDate = (String) duLieu.get("tu_ngay");
            String toDate = (String) duLieu.get("den_ngay");
            String status = duLieu.containsKey("tthai_lich") && duLieu.get("tthai_lich") != null
                    ? (String) duLieu.get("tthai_lich")
                    : "ACTIVE";

            if (month == null || month.isEmpty()) {
                log.warn("[SCHEDULE-KAFKA] Missing required thang_ck. Skipping event.");
                return;
            }
            if (fromDate == null || fromDate.isEmpty()) {
                fromDate = java.time.LocalDate.now().toString();
            }
            if (toDate == null || toDate.isEmpty()) {
                toDate = fromDate;
            }

            String maDdo = (String) event.get("ma_ddo");
            if (maDdo != null && !maDdo.isEmpty()) {
                cmisSyncRepository.upsertMeterPointSchedule(maDdo, month, period, fromDate, toDate, status);
                log.info("[SCHEDULE-KAFKA] Upserted meter point schedule via Repository: {}, Month: {}, Period: {}", maDdo, month, period);
            } else {
                String dtuongQly = (String) event.get("dtuong_qly");
                if (dtuongQly == null || dtuongQly.isEmpty()) {
                    log.warn("[SCHEDULE-KAFKA] Missing both ma_ddo and dtuong_qly. Skipping event.");
                    return;
                }

                int nMinus = duLieu.containsKey("n_tru") && duLieu.get("n_tru") != null ? ((Number) duLieu.get("n_tru")).intValue() : 1;
                int nPlus = duLieu.containsKey("n_cong") && duLieu.get("n_cong") != null ? ((Number) duLieu.get("n_cong")).intValue() : 1;
                int totalAccounts = duLieu.containsKey("tong_kh") ? ((Number) duLieu.get("tong_kh")).intValue() : 5;
                String maDviqly = (String) duLieu.getOrDefault("ma_dviqly", "PD0600");

                cmisSyncRepository.upsertDtuongQlySchedule(dtuongQly, month, period, fromDate, toDate, nMinus, nPlus, totalAccounts, maDviqly);
                log.info("[SCHEDULE-KAFKA] Upserted management object schedule via Repository: {}, Month: {}, Period: {}", dtuongQly, month, period);
            }
        } catch (Exception e) {
            log.error("[SCHEDULE-KAFKA] Failed to process schedule sync event: {}", e.getMessage(), e);
        }
    }
}
