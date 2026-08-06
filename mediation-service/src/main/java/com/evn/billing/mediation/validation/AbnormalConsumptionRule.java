package com.evn.billing.mediation.validation;

import com.evn.billing.common.dto.AnomalyResult;
import com.evn.billing.common.util.SmartAnomalyDetector;
import com.evn.billing.mediation.repository.AmrIngestionRepository;
import com.evn.billing.mediation.repository.ValidationQueryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.util.List;

@Component
@Order(80)
public class AbnormalConsumptionRule implements ValidationRule {

    @Autowired
    private AmrIngestionRepository repository;

    @Autowired
    private ValidationQueryRepository validationQueryRepository;

    private final SmartAnomalyDetector detector = new SmartAnomalyDetector();

    @Override
    public void check(String accountId, String month, int period, ValidationResult result) {
        BigDecimal currentSum = validationQueryRepository.getCurrentConsumptionSum(accountId, month, period);

        // 2. Get history of previous periods (up to 12)
        List<BigDecimal> history = repository.getHistoricalConsumptions(accountId, month, period);

        // 3. Detect anomaly
        AnomalyResult anomalyResult = detector.detect(currentSum, history);

        if (anomalyResult.isAnomaly()) {
            result.addError(String.format(
                    "ERR_ABNORMAL_SPIKE: Smart Anomaly detected for Account %s. Z-Score: %s, Current: %s, EMA: %s, StdDev: %s.",
                    accountId, anomalyResult.getZScore(), currentSum, anomalyResult.getEma(), anomalyResult.getStdDev()
            ));
        }
    }

    @Override
    public void check(String accountId, String month, int period, com.evn.billing.common.dto.BillingConfigSnapshot config, List<com.evn.billing.common.domain.MeterUsage> usages, ValidationResult result) {
        if (config == null) {
            check(accountId, month, period, result);
            return;
        }

        BigDecimal currentSum = BigDecimal.ZERO;
        if (usages != null) {
            for (com.evn.billing.common.domain.MeterUsage u : usages) {
                if (accountId.equals(u.getMaKhang()) && month.equals(u.getThangChuKy()) && period == u.getKyChot()
                        && ("VALIDATED".equals(u.getTrangThaiXuLy()) || "PENDING_MANUAL".equals(u.getTrangThaiXuLy()))) {
                    if (u.getConsumption() != null) {
                        currentSum = currentSum.add(u.getConsumption());
                    }
                }
            }
        }

        List<BigDecimal> history = repository.getHistoricalConsumptions(accountId, month, period);

        AnomalyResult anomalyResult = detector.detect(currentSum, history);

        if (anomalyResult.isAnomaly()) {
            result.addError(String.format(
                    "ERR_ABNORMAL_SPIKE: Smart Anomaly detected for Account %s. Z-Score: %s, Current: %s, EMA: %s, StdDev: %s.",
                    accountId, anomalyResult.getZScore(), currentSum, anomalyResult.getEma(), anomalyResult.getStdDev()
            ));
        }
    }
}
