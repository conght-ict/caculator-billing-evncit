package com.evn.billing.mediation.validation;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.domain.MeterUsage;
import java.util.List;

public interface ValidationRule {
    void check(String accountId, String month, int period, ValidationResult result);

    default void check(String accountId, String month, int period, BillingConfigSnapshot config, List<MeterUsage> usages, ValidationResult result) {
        check(accountId, month, period, result);
    }
}
