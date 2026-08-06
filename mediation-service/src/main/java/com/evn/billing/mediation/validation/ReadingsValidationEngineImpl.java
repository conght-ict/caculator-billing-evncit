package com.evn.billing.mediation.validation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import java.util.List;

@Service
public class ReadingsValidationEngineImpl implements ReadingsValidationEngine {

    @Autowired
    private List<ValidationRule> rules;

    @Autowired
    private com.evn.billing.mediation.repository.BillingAccountSnapshotRepository snapshotRepository;

    @Autowired
    private com.evn.billing.mediation.repository.MeterUsageRepository meterUsageRepository;

    @Autowired
    private org.springframework.data.redis.core.StringRedisTemplate redisTemplate;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    @Override
    public ValidationResult validate(String accountId, String month, int period) {
        BillingConfigSnapshot config = getSnapshotConfig(accountId, month, period);
        List<MeterUsage> usages = meterUsageRepository.findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(accountId, month, period, "VALIDATED");
        return validate(accountId, month, period, config, usages);
    }

    @Override
    public ValidationResult validate(String accountId, String month, int period, BillingConfigSnapshot config, List<MeterUsage> usages) {
        ValidationResult result = new ValidationResult();
        for (ValidationRule rule : rules) {
            rule.check(accountId, month, period, config, usages, result);
        }
        return result;
    }

    private BillingConfigSnapshot getSnapshotConfig(String accountId, String month, int period) {
        String cacheKey = "snapshot:" + accountId + ":" + month + ":" + period;
        try {
            String cachedJson = redisTemplate.opsForValue().get(cacheKey);
            if (cachedJson != null) {
                return objectMapper.readValue(cachedJson, BillingConfigSnapshot.class);
            }
        } catch (Exception e) {
            // Ignore
        }

        var snapshotOpt = snapshotRepository.findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(accountId, month, period, 1);
        if (snapshotOpt.isPresent()) {
            BillingConfigSnapshot config = snapshotOpt.get().getDuLieuCauHinh();
            try {
                redisTemplate.opsForValue().set(cacheKey, objectMapper.writeValueAsString(config), 24, java.util.concurrent.TimeUnit.HOURS);
            } catch (Exception e) {
                // Ignore
            }
            return config;
        }
        return null;
    }
}
