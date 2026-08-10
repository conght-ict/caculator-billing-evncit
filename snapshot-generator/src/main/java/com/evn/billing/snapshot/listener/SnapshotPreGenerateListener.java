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
public class SnapshotPreGenerateListener {

    @Autowired
    private SnapshotGeneratorService snapshotGeneratorService;

    @Autowired
    private DtuongQlyScheduleRepository dtuongQlyScheduleRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "snapshot-pre-generate-topic",
            groupId = "snapshot-pregen-group"
    )
    public void onPreGenerate(String message) {
        log.info("[PRE-GEN-KAFKA] Received pre-generate event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String dtuongQly = (String) event.get("dtuong_qly");
            String month = (String) event.get("thang_chu_ky");
            Integer period = event.containsKey("ky_chot") && event.get("ky_chot") != null
                    ? ((Number) event.get("ky_chot")).intValue()
                    : 1;

            if (dtuongQly == null || dtuongQly.isEmpty() || month == null || month.isEmpty()) {
                log.warn("[PRE-GEN-KAFKA] Missing dtuong_qly or thang_chu_ky. Skipping.");
                return;
            }

            log.info("[PRE-GEN-KAFKA] Generating snapshots in bulk for book: {}, month: {}, period: {}", dtuongQly, month, period);
            snapshotGeneratorService.generateSnapshotsForBook(dtuongQly, month, period);
            log.info("[PRE-GEN-KAFKA] Finished bulk snapshot generation. Updating schedule state...");

            try {
                Optional<DtuongQlySchedule> schedOpt = dtuongQlyScheduleRepository
                        .findByDtuongQlyAndThangCkAndKyChot(dtuongQly, month, period);
                if (schedOpt.isPresent()) {
                    DtuongQlySchedule sched = schedOpt.get();
                    sched.setSnapshotGenerated(true);
                    sched.setSnapshotGeneratedAt(LocalDateTime.now());
                    dtuongQlyScheduleRepository.save(sched);
                    log.info("[PRE-GEN-KAFKA] Updated snapshot_generated = true for book: {}", dtuongQly);
                }
            } catch (Exception ex) {
                log.error("[PRE-GEN-KAFKA] Failed to update snapshot_generated state for book: {}", dtuongQly, ex);
            }
        } catch (Exception e) {
            log.error("[PRE-GEN-KAFKA] Failed to process pre-generate event: {}", e.getMessage(), e);
        }
    }
}
