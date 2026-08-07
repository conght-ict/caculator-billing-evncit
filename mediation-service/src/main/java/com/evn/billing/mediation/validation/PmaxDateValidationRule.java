package com.evn.billing.mediation.validation;

import com.evn.billing.mediation.repository.ValidationQueryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Component
@Order(60)
public class PmaxDateValidationRule implements ValidationRule {

    @Autowired
    private ValidationQueryRepository validationQueryRepository;

    @Override
    public void check(String maKhang, String month, int period, ValidationResult result) {
        List<Map<String, Object>> readings = validationQueryRepository.findNonReplacedReadings(maKhang, month, period);
        
        if (month == null || !month.contains("_")) {
            throw new IllegalArgumentException("Billing cycle month is missing or invalid: " + month);
        }
        int targetYear;
        int targetMonth;
        try {
            String[] parts = month.split("_");
            targetYear = Integer.parseInt(parts[0]);
            targetMonth = Integer.parseInt(parts[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse billing cycle month: " + month, e);
        }

        for (Map<String, Object> r : readings) {
            String bcs = (String) r.get("tgian_bdien");
            if (bcs != null && bcs.equalsIgnoreCase("PMAX")) {
                Timestamp denNgayTs = (Timestamp) r.get("den_ngay");
                if (denNgayTs != null) {
                    LocalDate pmaxDate = denNgayTs.toLocalDateTime().toLocalDate();
                    if (pmaxDate.getYear() != targetYear || pmaxDate.getMonthValue() != targetMonth) {
                        result.addError(String.format("ERR_PMAX_DATE_INVALID: Pmax date %s of meter %s is outside target billing month %s",
                                pmaxDate, r.get("ma_cto"), month));
                    }
                }
            }
        }
    }

    @Override
    public void check(String maKhang, String month, int period, com.evn.billing.common.dto.BillingConfigSnapshot config, List<com.evn.billing.common.domain.MeterUsage> usages, ValidationResult result) {
        if (config == null) {
            check(maKhang, month, period, result);
            return;
        }

        if (month == null || !month.contains("_")) {
            throw new IllegalArgumentException("Billing cycle month is missing or invalid: " + month);
        }
        int targetYear;
        int targetMonth;
        try {
            String[] parts = month.split("_");
            targetYear = Integer.parseInt(parts[0]);
            targetMonth = Integer.parseInt(parts[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse billing cycle month: " + month, e);
        }

        if (usages == null || usages.isEmpty()) {
            return;
        }

        for (com.evn.billing.common.domain.MeterUsage u : usages) {
            if (maKhang.equals(u.getMaKhang()) && month.equals(u.getThangChuKy()) && period == u.getKyChot() && !"REPLACED".equals(u.getTrangThaiXuLy())) {
                String bcs = u.getTgianBdien();
                if (bcs != null && bcs.equalsIgnoreCase("PMAX")) {
                    java.time.LocalDateTime denNgay = u.getDenNgay();
                    if (denNgay != null) {
                        LocalDate pmaxDate = denNgay.toLocalDate();
                        if (pmaxDate.getYear() != targetYear || pmaxDate.getMonthValue() != targetMonth) {
                            result.addError(String.format("ERR_PMAX_DATE_INVALID: Pmax date %s of meter %s is outside target billing month %s",
                                    pmaxDate, u.getMaCto(), month));
                        }
                    }
                }
            }
        }
    }
}
