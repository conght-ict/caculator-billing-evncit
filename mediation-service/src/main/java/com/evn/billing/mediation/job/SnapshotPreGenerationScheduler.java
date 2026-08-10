package com.evn.billing.mediation.job;

import com.evn.billing.mediation.repository.CmisSyncRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class SnapshotPreGenerationScheduler {

    @Autowired
    private CmisSyncRepository cmisSyncRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${billing.snapshot.pre-gen-days:2}")
    private int preGenDays;

    /**
     * Runs daily at 02:00 AM to check and pre-generate snapshots.
     */
    @Scheduled(cron = "0 0 2 * * *")
    public void triggerPendingPreGenerations() {
        log.info("[PRE-GEN-SCHEDULER] Checking books requiring snapshot pre-generation...");
        try {
            List<Map<String, Object>> dueSoons = cmisSyncRepository.findBooksWithUpcomingMeterReadings(preGenDays);
            if (dueSoons.isEmpty()) {
                log.info("[PRE-GEN-SCHEDULER] No books found requiring snapshot pre-generation.");
                return;
            }

            log.info("[PRE-GEN-SCHEDULER] Found {} books for snapshot pre-generation. Dispatching events...", dueSoons.size());
            for (Map<String, Object> book : dueSoons) {
                String dtuongQly = (String) book.get("dtuong_qly");
                String month = (String) book.get("thang_ck");
                Integer period = ((Number) book.get("ky_chot")).intValue();
                String denNgay = book.get("den_ngay").toString();

                Map<String, Object> preGenEvent = new HashMap<>();
                preGenEvent.put("dtuong_qly", dtuongQly);
                preGenEvent.put("thang_chu_ky", month);
                preGenEvent.put("ky_chot", period);
                preGenEvent.put("den_ngay", denNgay);
                preGenEvent.put("trigger_source", "SCHEDULE_ROUTINE");

                kafkaTemplate.send("snapshot-pre-generate-topic", dtuongQly, preGenEvent);
                log.info("[PRE-GEN-SCHEDULER] Dispatched pre-gen event for book: {}, month: {}, period: {}, den_ngay: {}", dtuongQly, month, period, denNgay);
            }
        } catch (Exception e) {
            log.error("[PRE-GEN-SCHEDULER] Error during snapshot pre-generation routine", e);
        }
    }
}
