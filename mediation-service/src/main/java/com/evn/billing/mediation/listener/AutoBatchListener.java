package com.evn.billing.mediation.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import com.evn.billing.mediation.service.BatchService;

@Component
@Slf4j
public class AutoBatchListener {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job billingJob;

    @Autowired
    private BatchService batchService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "billing-auto-batch-topic",
            groupId = "auto-batch-trigger-group",
            properties = "value.deserializer=StringDeserializer"
    )
    public void listenAutoBatchTrigger(String message) {
        log.info("[AUTO-BATCH-KAFKA] Received auto-batch trigger event: {}", message);
        try {
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            String dtuongQly = (String) event.get("dtuongQly");
            String month = (String) event.get("month");
            Integer period = ((Number) event.get("period")).intValue();

            if (batchService.isBookAlreadyCompleted(dtuongQly, month, period)) {
                log.warn("[AUTO-BATCH-KAFKA] Book: {}, Month: {}, Period: {} already COMPLETED. Skipping duplicate trigger.",
                        dtuongQly, month, period);
                return;
            }

            log.info("[AUTO-BATCH-KAFKA] Launching Spring Batch Job automatically for Book: {}, Month: {}, Period: {}", 
                    dtuongQly, month, period);

            Map<String, JobParameter<?>> params = new HashMap<>();
            params.put("dtuongQly", new JobParameter<>(dtuongQly, String.class));
            params.put("month", new JobParameter<>(month, String.class));
            params.put("period", new JobParameter<>(period.longValue(), Long.class));
            params.put("calculationVersion", new JobParameter<>(1L, Long.class)); // version is resolved dynamically per account inside the batch processor
            params.put("time", new JobParameter<>(System.currentTimeMillis(), Long.class));

            JobParameters jobParameters = new JobParameters(params);
            JobExecution execution = jobLauncher.run(billingJob, jobParameters);

            log.info("[AUTO-BATCH-KAFKA] Job initiated. Execution ID: {}, Status: {}", execution.getId(), execution.getStatus());
        } catch (Exception e) {
            log.error("[AUTO-BATCH-KAFKA] Failed to launch auto batch job: {}", e.getMessage(), e);
        }
    }
}
