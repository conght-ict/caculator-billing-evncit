package com.evn.billing.mediation.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class SnapshotEventPublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${billing.snapshot.recreate-topic:snapshot-recreate-topic}")
    private String recreateTopic;

    public void publishAccountRecreate(String maKhang, String dtuongQly,
            String month, int period, String ruleId, String bangNguon, String truongThayDoi) {
        Map<String, Object> event = Map.of(
            "loai", "ACCOUNT",
            "ma_khang", maKhang,
            "dtuong_qly", dtuongQly,
            "thang_chu_ky", month,
            "ky_chot", period,
            "rule_id", ruleId,
            "bang_nguon", bangNguon,
            "truong_thay_doi", truongThayDoi
        );
        kafkaTemplate.send(recreateTopic, maKhang, event);
        log.info("[SNAP-PUBLISH] Account recreate event sent for: {} (Topic: {})", maKhang, recreateTopic);
    }

    public void publishBookRecreate(String dtuongQly, String month, int period) {
        Map<String, Object> event = Map.of(
            "loai", "BOOK",
            "dtuong_qly", dtuongQly,
            "thang_chu_ky", month,
            "ky_chot", period,
            "rule_id", "R-06"
        );
        kafkaTemplate.send(recreateTopic, dtuongQly, event);
        log.info("[SNAP-PUBLISH] Book recreate event sent for: {} (Topic: {})", dtuongQly, recreateTopic);
    }
}
