package com.evn.billing.mediation.service;

import com.evn.billing.mediation.dto.CmisReadingEvent;
import com.evn.billing.mediation.job.OracleAmrIngestionJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class ReadingsIngestionService {

    private static final Logger log = LoggerFactory.getLogger(ReadingsIngestionService.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private OracleAmrIngestionJob oracleAmrIngestionJob;

    public void publishReadingsToKafka(List<CmisReadingEvent> readings) {
        for (CmisReadingEvent event : readings) {
            if (event.getMaKhang() != null) {
                kafkaTemplate.send("meter-readings-input", event.getMaKhang(), event);
            }
        }
        log.info("[READINGS-INGEST] Published {} readings to Kafka", readings.size());
    }

    public void triggerAmrIngestion(String dtuongQly, String month, Integer period) {
        oracleAmrIngestionJob.runIngestionForBook(dtuongQly, month, period);
        log.info("[AMR-INGEST] Triggered Oracle AMR ingestion for book: {}", dtuongQly);
    }
}
