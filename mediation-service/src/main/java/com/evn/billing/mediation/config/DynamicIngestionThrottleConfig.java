package com.evn.billing.mediation.config;

import lombok.Data;
import org.springframework.stereotype.Component;
import java.time.LocalTime;

@Component
@Data
public class DynamicIngestionThrottleConfig {

    @Data
    public static class IngestionProfile {
        private final int maxConcurrency;
        private final int batchSize;
        private final double rateLimitPerSecond; // 0 means unlimited
        private final String name;
    }

    private static final IngestionProfile OFF_PEAK_PROFILE = new IngestionProfile(20, 5000, 0.0, "OFF_PEAK");
    private static final IngestionProfile PEAK_PROFILE = new IngestionProfile(2, 500, 200.0, "PEAK");
    private static final IngestionProfile PANIC_PROFILE = new IngestionProfile(15, 3000, 1500.0, "SLA_PANIC");

    /**
     * Dynamically resolves ingestion profile based on current time AND remaining readings against SLA deadline.
     */
    public IngestionProfile getProfile(long pendingReadings, long secondsToSla) {
        if (secondsToSla > 0) {
            double requiredRate = (double) pendingReadings / secondsToSla;
            // If the required rate to meet SLA is greater than the standard peak rate limit, activate Panic Mode
            if (requiredRate > 200.0) {
                return PANIC_PROFILE;
            }
        }

        LocalTime now = LocalTime.now();
        // Off-peak hours from 02:00 AM to 07:00 AM
        if (now.isAfter(LocalTime.of(2, 0)) && now.isBefore(LocalTime.of(7, 0))) {
            return OFF_PEAK_PROFILE;
        }
        return PEAK_PROFILE;
    }
}
