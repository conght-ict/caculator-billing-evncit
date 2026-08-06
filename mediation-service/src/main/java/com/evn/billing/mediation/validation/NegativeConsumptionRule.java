package com.evn.billing.mediation.validation;

import com.evn.billing.mediation.repository.ValidationQueryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
@Order(20)
public class NegativeConsumptionRule implements ValidationRule {

    @Autowired
    private ValidationQueryRepository validationQueryRepository;

    @Override
    public void check(String accountId, String month, int period, ValidationResult result) {
        List<Map<String, Object>> list = validationQueryRepository.findNonReplacedReadings(accountId, month, period);
        for (Map<String, Object> r : list) {
            BigDecimal consumption = (BigDecimal) r.get("san_luong_tho");
            if (consumption != null && consumption.compareTo(BigDecimal.ZERO) < 0) {
                result.addError(String.format("ERR_NEGATIVE_CONSUMPTION: Meter point %s, register %s, meter %s has negative consumption: %s",
                        r.get("ma_ddo"), r.get("tgian_bdien"), r.get("ma_cto"), consumption));
            }
        }
    }

    @Override
    public void check(String accountId, String month, int period, com.evn.billing.common.dto.BillingConfigSnapshot config, List<com.evn.billing.common.domain.MeterUsage> usages, ValidationResult result) {
        if (config == null) {
            check(accountId, month, period, result);
            return;
        }

        if (usages == null || usages.isEmpty()) {
            return;
        }

        for (com.evn.billing.common.domain.MeterUsage u : usages) {
            if (accountId.equals(u.getMaKhang()) && month.equals(u.getThangChuKy()) && period == u.getKyChot() && !"REPLACED".equals(u.getTrangThaiXuLy())) {
                BigDecimal consumption = u.getConsumption();
                if (consumption != null && consumption.compareTo(BigDecimal.ZERO) < 0) {
                    result.addError(String.format("ERR_NEGATIVE_CONSUMPTION: Meter point %s, register %s, meter %s has negative consumption: %s",
                            u.getMaDdo(), u.getTgianBdien(), u.getMaCto(), consumption));
                }
            }
        }
    }
}
