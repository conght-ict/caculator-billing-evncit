package com.evn.billing.snapshot.listener;

import com.evn.billing.common.domain.DtuongQlySchedule;
import com.evn.billing.snapshot.repository.DtuongQlyScheduleRepository;
import com.evn.billing.snapshot.service.SnapshotGeneratorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

@Component
@Slf4j
public class SnapshotRecreateListener {

    @Autowired
    private SnapshotGeneratorService snapshotGeneratorService;

    @Autowired
    private DtuongQlyScheduleRepository dtuongQlyScheduleRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "snapshot-recreate-topic",
            groupId = "snapshot-recreate-group"
    )
    public void onRecreate(String message) {
        log.info("[SNAP-RECREATE-KAFKA] Received snapshot recreate event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String loai = (String) event.get("loai");
            String dtuongQly = (String) event.get("dtuong_qly");
            String month = (String) event.get("thang_chu_ky");
            Integer period = event.containsKey("ky_chot") && event.get("ky_chot") != null
                    ? ((Number) event.get("ky_chot")).intValue()
                    : 1;

            if (month == null || month.isEmpty()) {
                log.warn("[SNAP-RECREATE-KAFKA] Missing thang_chu_ky. Skipping.");
                return;
            }

            if ("BOOK".equals(loai)) {
                if (dtuongQly == null || dtuongQly.isEmpty()) {
                    log.warn("[SNAP-RECREATE-KAFKA] Missing dtuong_qly for BOOK level recreate.");
                    return;
                }
                log.info("[SNAP-RECREATE-KAFKA] Regenerating snapshots in bulk for book: {}, month: {}, period: {}", dtuongQly, month, period);
                snapshotGeneratorService.generateSnapshotsForBook(dtuongQly, month, period);
                log.info("[SNAP-RECREATE-KAFKA] Finished bulk snapshot regeneration. Updating schedule state...");

                try {
                    Optional<DtuongQlySchedule> schedOpt = dtuongQlyScheduleRepository
                            .findByDtuongQlyAndThangCkAndKyChot(dtuongQly, month, period);
                    if (schedOpt.isPresent()) {
                        DtuongQlySchedule sched = schedOpt.get();
                        sched.setSnapshotGenerated(true);
                        sched.setSnapshotGeneratedAt(LocalDateTime.now());
                        dtuongQlyScheduleRepository.save(sched);
                        log.info("[SNAP-RECREATE-KAFKA] Updated snapshot_generated = true for book: {}", dtuongQly);
                    }
                } catch (Exception ex) {
                    log.error("[SNAP-RECREATE-KAFKA] Failed to update snapshot_generated state for book: {}", dtuongQly, ex);
                }
            } else {
                String maKhang = (String) event.get("ma_khang");
                if (maKhang == null || maKhang.isEmpty()) {
                    log.warn("[SNAP-RECREATE-KAFKA] Missing ma_khang for ACCOUNT level recreate.");
                    return;
                }
                String ruleId = event.containsKey("rule_id") ? (String) event.get("rule_id") : "R-01";
                String bangNguon = event.containsKey("bang_nguon") ? (String) event.get("bang_nguon") : "diem_do";
                String truongThayDoi = event.containsKey("truong_thay_doi") ? (String) event.get("truong_thay_doi") : "danh_sach_ap_gia";

                log.info("[SNAP-RECREATE-KAFKA] Regenerating snapshot for account: {}, month: {}, period: {}", maKhang, month, period);
                snapshotGeneratorService.generateSnapshotForAccount(maKhang, month, period, ruleId, bangNguon, truongThayDoi);
            }
        } catch (Exception e) {
            log.error("[SNAP-RECREATE-KAFKA] Failed to process recreate event: {}", e.getMessage(), e);
        }
    }
}
