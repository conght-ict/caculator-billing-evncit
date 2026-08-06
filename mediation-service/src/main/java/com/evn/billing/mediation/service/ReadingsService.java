package com.evn.billing.mediation.service;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import com.evn.billing.mediation.controller.MediationController;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class ReadingsService {

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    public int processAndSaveReadings(List<MediationController.ReadingDto> readings) {
        List<MeterUsage> batch = new ArrayList<>();

        for (MediationController.ReadingDto dto : readings) {
            int effectivePeriod = dto.getKyChot() != null ? dto.getKyChot() : 1;
            long generatedId = Math.abs((dto.getMaDdo() + "_" + dto.getThangChuKy()).hashCode());
            MeterUsageId compositeKey = new MeterUsageId(generatedId, 1, dto.getThangChuKy(), effectivePeriod);
            Optional<MeterUsage> existingOpt = meterUsageRepository.findById(compositeKey);

            if (existingOpt.isPresent()) {
                continue;
            }

            MeterUsage usage = new MeterUsage();
            usage.setIdChiSo(generatedId);
            usage.setLanDocPhu(1);
            usage.setKyChot(effectivePeriod);
            usage.setMaKhang(dto.getMaKhang());
            usage.setMaDdo(dto.getMaDdo());
            usage.setThangChuKy(dto.getThangChuKy());
            usage.setTuNgay(dto.getTuNgay());
            usage.setDenNgay(dto.getDenNgay());
            usage.setChiSoDau(dto.getChiSoDau());
            usage.setChiSoCuoi(dto.getChiSoCuoi());

            boolean indexDropped = dto.getChiSoCuoi().compareTo(dto.getChiSoDau()) < 0;
            usage.setCoQuayVong(indexDropped);

            BigDecimal rawCons;
            if (indexDropped) {
                double startVal = dto.getChiSoDau().doubleValue();
                double digits = Math.ceil(Math.log10(startVal));
                if (digits <= 0) digits = 5;
                BigDecimal maxVal = BigDecimal.valueOf(Math.pow(10, digits));
                usage.setMaxRegisterSnapshot(maxVal);
                rawCons = maxVal.subtract(dto.getChiSoDau()).add(dto.getChiSoCuoi());
            } else {
                usage.setMaxRegisterSnapshot(new BigDecimal("99999.9"));
                rawCons = dto.getChiSoCuoi().subtract(dto.getChiSoDau());
            }

            usage.setSanLuongTho(rawCons);
            usage.setTrangThaiXuLy("VALIDATED");
            usage.setLoaiGhiIndex("ORIGINAL");
            usage.setNguonGhi("AMR");
            usage.setCreatedAt(LocalDateTime.now());

            batch.add(usage);
        }

        if (!batch.isEmpty()) {
            meterUsageRepository.saveAll(batch);
        }
        return batch.size();
    }
}
