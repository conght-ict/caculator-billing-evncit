package com.evn.billing.mediation.validation;

import com.evn.billing.mediation.repository.ValidationQueryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
@Order(70)
public class NpcDecimalTruncationRule implements ValidationRule {

    @Autowired
    private ValidationQueryRepository validationQueryRepository;

    @Override
    public void check(String accountId, String month, int period, ValidationResult result) {
        String maDviqly = validationQueryRepository.findMaDviqlyByAccount(accountId);
        if (maDviqly == null) {
            return;
        }
        String tct = com.evn.billing.common.util.UnitUtility.getMadviCapTctFromMaDviQly(maDviqly);
        if (!"PA".equals(tct)) {
            return; // Bỏ qua nếu không phải đơn vị thuộc EVN NPC (mã TCT đại diện là 'PA')
        }

        List<Map<String, Object>> readings = validationQueryRepository.findNonReplacedReadings(accountId, month, period);
        for (Map<String, Object> r : readings) {
            String bcs = (String) r.get("tgian_bdien");
            if (bcs != null && (bcs.equals("KT") || bcs.equals("BT") || bcs.equals("CD") || bcs.equals("TD"))) {
                BigDecimal index = (BigDecimal) r.get("chi_so_cuoi");
                if (index != null && index.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
                    result.addError(String.format("ERR_NPC_DECIMAL_VIOLATION: Meter %s register %s has decimal digits when heSoNhan requires truncation.",
                            r.get("ma_cto"), bcs));
                }
            }
        }
    }

    @Override
    public void check(String accountId, String month, int period, com.evn.billing.common.dto.BillingConfigSnapshot config, List<com.evn.billing.common.domain.MeterUsage> usages, ValidationResult result) {
        if (config == null) {
            check(accountId, month, period, result);
            return;
        }

        String maDviqly = config.getMaDviqly();
        if (maDviqly == null) {
            return;
        }

        String tct = com.evn.billing.common.util.UnitUtility.getMadviCapTctFromMaDviQly(maDviqly);
        if (!"PA".equals(tct)) {
            return; // Bỏ qua nếu không phải đơn vị thuộc EVN NPC
        }

        if (usages == null || usages.isEmpty()) {
            return;
        }

        for (com.evn.billing.common.domain.MeterUsage u : usages) {
            if (accountId.equals(u.getMaKhang()) && month.equals(u.getThangChuKy()) && period == u.getKyChot() && !"REPLACED".equals(u.getTrangThaiXuLy())) {
                String bcs = u.getTgianBdien();
                if (bcs != null && (bcs.equals("KT") || bcs.equals("BT") || bcs.equals("CD") || bcs.equals("TD"))) {
                    BigDecimal index = u.getChiSoCuoi();
                    if (index != null && index.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
                        result.addError(String.format("ERR_NPC_DECIMAL_VIOLATION: Meter %s register %s has decimal digits when heSoNhan requires truncation.",
                                u.getMaCto(), bcs));
                    }
                }
            }
        }
    }
}
