package com.evn.billing.mediation.controller;

import com.evn.billing.mediation.dto.CmisReadingEvent;
import com.evn.billing.common.dto.IngestAmrRequest;
import com.evn.billing.mediation.service.ReadingsIngestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class ReadingsIngestionController {

    @Autowired
    private ReadingsIngestionService readingsIngestionService;

    @PostMapping("/readings")
    public ResponseEntity<String> ingestReadings(@RequestBody ReadingsPayload payload) {
        if (payload == null || payload.getReadings() == null || payload.getReadings().isEmpty()) {
            return ResponseEntity.badRequest().body("Payload readings list is empty.");
        }
        readingsIngestionService.publishReadingsToKafka(payload.getReadings());
        return ResponseEntity.accepted().body("Readings accepted and sent to processing queue.");
    }

    @PostMapping("/readings/ingest")
    public ResponseEntity<String> manualIngestAmr(@RequestBody IngestAmrRequest request) {
        readingsIngestionService.triggerAmrIngestion(
                request.getDtuongQly(),
                request.getThangChuKy(),
                request.getKyChot()
        );
        return ResponseEntity.ok("Oracle AMR Ingestion triggered successfully for book: " + request.getDtuongQly());
    }

    @lombok.Data
    public static class ReadingsPayload {
        private List<CmisReadingEvent> readings;
    }
}
