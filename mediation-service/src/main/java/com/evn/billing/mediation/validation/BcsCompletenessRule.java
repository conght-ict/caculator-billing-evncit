package com.evn.billing.mediation.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.evn.billing.mediation.repository.ValidationQueryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
@Order(10)
public class BcsCompletenessRule implements ValidationRule {

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
        
        java.time.LocalDate targetDate = ((java.sql.Date) denNgay).toLocalDate();

        List<Map<String, Object>> received = validationQueryRepository.findValidatedReadings(maKhang, month, period);

        Set<String> receivedKeys = new HashSet<>();
        for (Map<String, Object> r : received) {
            String mId = (String) r.get("ma_ddo");
            String bcs = (String) r.get("tgian_bdien");
            String maCto = (String) r.get("ma_cto");
            if (maCto == null) maCto = "UNKNOWN";
            receivedKeys.add(mId + ":" + bcs + ":" + maCto);
        }

        for (Map<String, Object> mp : meterPoints) {
            String meterPointId = (String) mp.get("ma_ddo");
            int loaiDdo = ((Number) mp.get("loai_ddo")).intValue();
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

            if (meterList.isEmpty()) {
                Map<String, Object> dummy = new HashMap<>();
                dummy.put("so_seri", "UNKNOWN");
                dummy.put("ma_cto", "UNKNOWN");
                dummy.put("trang_thai", "ACTIVE");
                dummy.put("danh_sach_bcs", lookupBcsFromLoaiDdo(loaiDdo));
                meterList = List.of(dummy);
            }

            for (Map<String, Object> cto : meterList) {
                String ctoStatus = (String) cto.get("trang_thai");
                String ngayTreoStr = (String) cto.get("ngay_treo");
                String ngayThaoStr = (String) cto.get("ngay_thao");
                java.time.LocalDate ngayTreo = ngayTreoStr != null ? java.time.LocalDate.parse(ngayTreoStr) : null;
                java.time.LocalDate ngayThao = ngayThaoStr != null ? java.time.LocalDate.parse(ngayThaoStr) : null;

                boolean isActive = false;
                if ("ACTIVE".equalsIgnoreCase(ctoStatus)) {
                    isActive = true;
                } else {
                    java.time.LocalDate periodStart = targetDate.minusDays(30);
                    if (ngayThao != null && !ngayThao.isBefore(periodStart)) {
                        isActive = true;
                    }
                }

                if (isActive) {
                    List<String> bcsList = (List<String>) cto.get("danh_sach_bcs");
                    if (bcsList == null || bcsList.isEmpty()) {
                        bcsList = lookupBcsFromLoaiDdo(loaiDdo);
                    }
                    String maCto = (String) cto.getOrDefault("ma_cto", cto.get("so_seri"));
                    if (maCto == null) maCto = "UNKNOWN";

                    for (String bcs : bcsList) {
                        String requiredKey = meterPointId + ":" + bcs + ":" + maCto;
                        if (!receivedKeys.contains(requiredKey)) {
                            result.addError(String.format("ERR_MISSING_BCS_READING: Meter point %s, meter %s register %s is missing.",
                                    meterPointId, maCto, bcs));
                        }
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

        java.time.LocalDate targetDate = config.getDenNgay();
        if (targetDate == null) {
            throw new IllegalStateException("Snapshot configuration is missing periodToDate for account: " + config.getMaKhang());
        }

        Set<String> receivedKeys = new HashSet<>();
        if (usages != null) {
            for (com.evn.billing.common.domain.MeterUsage u : usages) {
                if (maKhang.equals(u.getMaKhang()) && month.equals(u.getThangChuKy()) && period == u.getKyChot() && "VALIDATED".equals(u.getTrangThaiXuLy())) {
                    String mId = u.getMaDdo();
                    String bcs = u.getTgianBdien();
                    String maCto = u.getMaCto();
                    if (maCto == null) maCto = "UNKNOWN";
                    receivedKeys.add(mId + ":" + bcs + ":" + maCto);
                }
            }
        }

        if (config.getMeterTopology() == null || config.getMeterTopology().getRootPoints() == null) {
            return;
        }

        for (com.evn.billing.common.dto.MeterPointNode node : config.getMeterTopology().getRootPoints()) {
            checkNodeCompleteness(node, targetDate, receivedKeys, result);
        }
    }

    private void checkNodeCompleteness(com.evn.billing.common.dto.MeterPointNode node, java.time.LocalDate targetDate, Set<String> receivedKeys, ValidationResult result) {
        String meterPointId = node.getMaDdo();
        Short loaiDdoShort = node.getLoaiDdo();
        int loaiDdo = loaiDdoShort != null ? loaiDdoShort.intValue() : 1;
        List<com.evn.billing.common.dto.MeterDetails> activeMeters = node.getActiveMeters();

        List<com.evn.billing.common.dto.MeterDetails> meterList = new ArrayList<>();
        if (activeMeters != null) {
            meterList.addAll(activeMeters);
        }

        if (meterList.isEmpty()) {
            com.evn.billing.common.dto.MeterDetails dummy = new com.evn.billing.common.dto.MeterDetails();
            dummy.setSoSeri("UNKNOWN");
            dummy.setMaCto("UNKNOWN");
            dummy.setTrangThai("ACTIVE");
            dummy.setDanhSachBcs(lookupBcsFromLoaiDdo(loaiDdo));
            meterList = List.of(dummy);
        }

        for (com.evn.billing.common.dto.MeterDetails cto : meterList) {
            String ctoStatus = cto.getTrangThai();
            java.time.LocalDate ngayTreo = cto.getNgayTreo();
            java.time.LocalDate ngayThao = cto.getNgayThao();

            boolean isActive = false;
            if ("ACTIVE".equalsIgnoreCase(ctoStatus)) {
                isActive = true;
            } else {
                java.time.LocalDate periodStart = targetDate.minusDays(30);
                if (ngayThao != null && !ngayThao.isBefore(periodStart)) {
                    isActive = true;
                }
            }

            if (isActive) {
                List<String> bcsList = cto.getDanhSachBcs();
                if (bcsList == null || bcsList.isEmpty()) {
                    bcsList = lookupBcsFromLoaiDdo(loaiDdo);
                }
                String maCto = cto.getMaCto();
                if (maCto == null) maCto = cto.getSoSeri();
                if (maCto == null) maCto = "UNKNOWN";

                for (String bcs : bcsList) {
                    String requiredKey = meterPointId + ":" + bcs + ":" + maCto;
                    if (!receivedKeys.contains(requiredKey)) {
                        result.addError(String.format("ERR_MISSING_BCS_READING: Meter point %s, meter %s register %s is missing.",
                                meterPointId, maCto, bcs));
                    }
                }
            }
        }

        if (node.getChildPoints() != null) {
            for (com.evn.billing.common.dto.MeterPointNode child : node.getChildPoints()) {
                checkNodeCompleteness(child, targetDate, receivedKeys, result);
            }
        }
    }

    private List<String> lookupBcsFromLoaiDdo(int loaiDdo) {
        switch (loaiDdo) {
            case 2: return List.of("BT", "TD");
            case 3: return List.of("BT", "CD", "TD");
            case 4: return List.of("KT", "VC");
            case 5: return List.of("BT", "TD", "VC");
            case 6: return List.of("BT", "CD", "TD", "VC");
            default: return List.of("KT");
        }
    }
}
