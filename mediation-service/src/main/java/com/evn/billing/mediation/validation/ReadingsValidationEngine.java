package com.evn.billing.mediation.validation;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import java.util.List;

public interface ReadingsValidationEngine {
    ValidationResult validate(String maKhang, String month, int period);
    ValidationResult validate(String maKhang, String month, int period, BillingConfigSnapshot config, List<MeterUsage> usages);
}
