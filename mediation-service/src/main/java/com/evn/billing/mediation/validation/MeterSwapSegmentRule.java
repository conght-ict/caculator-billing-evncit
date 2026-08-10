package com.evn.billing.mediation.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.evn.billing.mediation.repository.ValidationQueryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.*;
import java.time.LocalDate;

@Component
@Order(30)
public class MeterSwapSegmentRule implements ValidationRule {

    @Autowired
    private ValidationQueryRepository validationQueryRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void check(String maKhang, String month, int period, ValidationResult result) {
        List<Map<String, Object>> meterPoints = validationQueryRepository.findActiveMeterPointsByAccount(maKhang);

        Date denNgay = null;
        try {
            denNgay = validationQueryRepository.findDenNgayByDdoSchedule(maKhang, month, period);
        } catch (Exception e) {
            try {
                denNgay = validationQueryRepository.findDenNgayByDqlySchedule(maKhang, month, period);
            } catch (Exception ex) {
                throw new IllegalStateException("Billing cycle end date (den_ngay) is missing and cannot be resolved for account: " + maKhang);
            }
        }
        
        LocalDate targetDate = ((java.sql.Date) denNgay).toLocalDate();
        LocalDate periodStart = targetDate.minusDays(30);

        for (Map<String, Object> mp : meterPoints) {
            String meterPointId = (String) mp.get("ma_ddo");
            String infoCto = mp.get("thong_tin_cto") != null ? mp.get("thong_tin_cto").toString() : null;

            List<Map<String, Object>> meterList = new ArrayList<>();
            if (infoCto != null && !infoCto.trim().isEmpty() && !infoCto.trim().equals("[]") && !infoCto.trim().equals("{}")) {
                try {
                    if (infoCto.trim().startsWith("[")) {
                        meterList = objectMapper.readValue(infoCto, List.class);
                    } else {
                        meterList = List.of(objectMapper.readValue(infoCto, Map.class));
                    }
                } catch (Exception e) {
                    // ignore
                }
            }

            List<String> activeMetersInPeriod = new ArrayList<>();
            for (Map<String, Object> cto : meterList) {
                String ctoStatus = (String) cto.get("trang_thai");
                String ngayThaoStr = (String) cto.get("ngay_thao");
                LocalDate ngayThao = ngayThaoStr != null ? LocalDate.parse(ngayThaoStr) : null;

                boolean isActive = "ACTIVE".equalsIgnoreCase(ctoStatus) || (ngayThao != null && !ngayThao.isBefore(periodStart));
                if (isActive) {
                    String maCto = (String) cto.getOrDefault("ma_cto", cto.get("so_seri"));
                    if (maCto != null) {
                        activeMetersInPeriod.add(maCto);
                    }
                }
            }

            if (activeMetersInPeriod.size() >= 2) {
                List<String> ingestedMeters = validationQueryRepository.findValidatedMetersByMeterPoint(meterPointId, month, period);

                for (String activeMeter : activeMetersInPeriod) {
                    if (!ingestedMeters.contains(activeMeter)) {
                        result.addError(String.format("ERR_INCOMPLETE_METER_SWAP_SEGMENTS: Meter point %s had swap but is missing readings for meter %s.",
                                meterPointId, activeMeter));
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

        LocalDate targetDate = config.getDenNgay();
        if (targetDate == null) {
            throw new IllegalStateException("Snapshot configuration is missing periodToDate for account: " + config.getMaKhang());
        }
        LocalDate periodStart = targetDate.minusDays(30);

        if (config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null) {
            return;
        }

        for (com.evn.billing.common.dto.MeterPointNode node : config.getMeterTopology().getRootPoints()) {
            checkNodeSwap(node, periodStart, month, period, usages, result);
        }
    }

    private void checkNodeSwap(com.evn.billing.common.dto.MeterPointNode node, LocalDate periodStart, String month, int period, List<com.evn.billing.common.domain.MeterUsage> usages, ValidationResult result) {
        String meterPointId = node.getMaDdo();
        List<com.evn.billing.common.dto.MeterDetails> activeMeters = node.getActiveMeters();

        List<String> activeMetersInPeriod = new ArrayList<>();
        if (activeMeters != null) {
            for (com.evn.billing.common.dto.MeterDetails cto : activeMeters) {
                String ctoStatus = cto.getTrangThai();
                LocalDate ngayThao = cto.getNgayThao();

                boolean isActive = "ACTIVE".equalsIgnoreCase(ctoStatus) || (ngayThao != null && !ngayThao.isBefore(periodStart));
                if (isActive) {
                    String maCto = cto.getMaCto();
                    if (maCto == null) maCto = cto.getSoSeri();
                    if (maCto != null) {
                        activeMetersInPeriod.add(maCto);
                    }
                }
            }
        }

        if (activeMetersInPeriod.size() >= 2) {
            Set<String> ingestedMeters = new HashSet<>();
            if (usages != null) {
                for (com.evn.billing.common.domain.MeterUsage u : usages) {
                    if (meterPointId.equals(u.getMaDdo()) && month.equals(u.getThangChuKy()) && period == u.getKyChot() && "VALIDATED".equals(u.getTrangThaiXuLy())) {
                        if (u.getMaCto() != null) {
                            ingestedMeters.add(u.getMaCto());
                        }
                    }
                }
            }

            for (String activeMeter : activeMetersInPeriod) {
                if (!ingestedMeters.contains(activeMeter)) {
                    result.addError(String.format("ERR_INCOMPLETE_METER_SWAP_SEGMENTS: Meter point %s had swap but is missing readings for meter %s.",
                            meterPointId, activeMeter));
                }
            }
        }

        if (node.getChildPoints() != null) {
            for (com.evn.billing.common.dto.MeterPointNode child : node.getChildPoints()) {
                checkNodeSwap(child, periodStart, month, period, usages, result);
            }
        }
    }
}
