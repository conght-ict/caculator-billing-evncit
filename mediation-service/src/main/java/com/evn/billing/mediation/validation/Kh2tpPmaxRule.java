package com.evn.billing.mediation.validation;

import com.evn.billing.mediation.repository.AmrIngestionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;

@Component
@Order(40)
public class Kh2tpPmaxRule implements ValidationRule {

    @Autowired
    private AmrIngestionRepository repository;

    @Override
    public void check(String accountId, String month, int period, ValidationResult result) {
        List<Map<String, Object>> missingMeters = repository.getKh2tpPmaxStatus(accountId, month, period);
        if (!missingMeters.isEmpty()) {
            for (Map<String, Object> meter : missingMeters) {
                String meterId = (String) meter.get("ma_ddo");
                if ("STACKING_GROUP".equals(meterId)) {
                    result.addError("ERR_MISSING_PMAX: Missing Pmax for stacking group of customer: " + accountId);
                } else {
                    result.addError("ERR_MISSING_PMAX: Meter point " + meterId + " is missing Pmax reading.");
                }
            }
        }
    }
}
