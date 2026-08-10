package com.evn.billing.mediation.validation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.TimeUnit;
import org.springframework.data.redis.core.StringRedisTemplate;

@Service
public class ReadingsValidationEngineImpl implements ReadingsValidationEngine {

    @Autowired
    private List<ValidationRule> rules;

    @Autowired
    private com.evn.billing.mediation.repository.BillingAccountSnapshotRepository snapshotRepository;

    @Autowired
    private com.evn.billing.mediation.repository.MeterUsageRepository meterUsageRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public ValidationResult validate(String maKhang, String month, int period) {
        BillingConfigSnapshot config = getSnapshotConfig(maKhang, month, period);
        List<MeterUsage> usages = meterUsageRepository.findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(maKhang, month, period, "VALIDATED");
        return validate(maKhang, month, period, config, usages);
    }

    @Override
    public ValidationResult validate(String maKhang, String month, int period, BillingConfigSnapshot config, List<MeterUsage> usages) {
        ValidationResult result = new ValidationResult();
        for (ValidationRule rule : rules) {
            rule.check(maKhang, month, period, config, usages, result);
        }
        return result;
    }

    private BillingConfigSnapshot getSnapshotConfig(String maKhang, String month, int period) {
        String cacheKey = "snapshot:" + maKhang + ":" + month + ":" + period;
        try {
            String cachedJson = redisTemplate.opsForValue().get(cacheKey);
            if (cachedJson != null) {
                return objectMapper.readValue(cachedJson, BillingConfigSnapshot.class);
            }
        } catch (Exception e) {
            // Ignore
        }

        var snapshotOpt = snapshotRepository.findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(maKhang, month, period, 1);
        if (snapshotOpt.isPresent()) {
            BillingConfigSnapshot config = snapshotOpt.get().getDuLieuCauHinh();
            try {
                redisTemplate.opsForValue().set(cacheKey, objectMapper.writeValueAsString(config), 24, TimeUnit.HOURS);
            } catch (Exception e) {
                // Ignore
            }
            return config;
        }
        return null;
    }
}
