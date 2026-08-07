package com.evn.billing.mediation.validation;

import com.evn.billing.mediation.repository.AmrIngestionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;

@Component
@Order(50)
public class CspkReactivePowerRule implements ValidationRule {

    @Autowired
    private AmrIngestionRepository repository;

    @Override
    public void check(String maKhang, String month, int period, ValidationResult result) {
        List<Map<String, Object>> reactiveViolations = repository.getReactivePowerStatus(maKhang, month, period);
        if (!reactiveViolations.isEmpty()) {
            for (Map<String, Object> violation : reactiveViolations) {
                String meterId = (String) violation.get("ma_ddo");
                result.addError("ERR_INVALID_REACTIVE_POWER: Meter point " + meterId + 
                        " has reactive power (VC > 0) but zero active power (HC <= 0).");
            }
        }
    }
}
