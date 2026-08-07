package com.evn.billing.mediation.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Repository
public class ValidationQueryRepositoryImpl implements ValidationQueryRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<Map<String, Object>> findActiveMeterPointsByAccount(String maKhang) {
        return jdbcTemplate.queryForList(
                "SELECT ma_ddo, loai_ddo, CAST(thong_tin_cto AS TEXT) AS thong_tin_cto FROM diem_do WHERE ma_khang = ? AND trang_thai = 'ACTIVE'",
                maKhang);
    }

    @Override
    public Date findDenNgayByDdoSchedule(String maKhang, String month, int period) {
        return jdbcTemplate.queryForObject(
                "SELECT den_ngay FROM lich_ghi_ddo WHERE ma_ddo = (SELECT ma_ddo FROM diem_do WHERE ma_khang = ? LIMIT 1) AND thang_ck = ? AND ky_chot = ?",
                Date.class, maKhang, month, period);
    }

    @Override
    public Date findDenNgayByDqlySchedule(String maKhang, String month, int period) {
        return jdbcTemplate.queryForObject(
                "SELECT den_ngay FROM lich_ghi_dqly WHERE dtuong_qly = (SELECT dtuong_qly FROM diem_do WHERE ma_khang = ? LIMIT 1) AND thang_ck = ? AND ky_chot = ?",
                Date.class, maKhang, month, period);
    }

    @Override
    public List<Map<String, Object>> findValidatedReadings(String maKhang, String month, int period) {
        return jdbcTemplate.queryForList(
                "SELECT ma_ddo, tgian_bdien, ma_cto FROM chi_so_dien_nang WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai_xu_ly = 'VALIDATED'",
                maKhang, month, period);
    }

    @Override
    public List<String> findValidatedMetersByMeterPoint(String meterPointId, String month, int period) {
        return jdbcTemplate.queryForList(
                "SELECT DISTINCT ma_cto FROM chi_so_dien_nang WHERE ma_ddo = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai_xu_ly = 'VALIDATED'",
                String.class, meterPointId, month, period);
    }

    @Override
    public List<Map<String, Object>> findNonReplacedReadings(String maKhang, String month, int period) {
        return jdbcTemplate.queryForList(
                "SELECT ma_ddo, tgian_bdien, chi_so_dau, chi_so_cuoi, san_luong_tho, ma_cto, den_ngay FROM chi_so_dien_nang WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai_xu_ly != 'REPLACED'",
                maKhang, month, period);
    }

    @Override
    public BigDecimal getCurrentConsumptionSum(String maKhang, String month, int period) {
        BigDecimal sum = jdbcTemplate.queryForObject(
                "SELECT SUM(san_luong_tho) FROM chi_so_dien_nang WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai_xu_ly IN ('VALIDATED', 'PENDING_MANUAL')",
                BigDecimal.class, maKhang, month, period);
        return sum != null ? sum : BigDecimal.ZERO;
    }

    @Override
    public String findMaDviqlyByAccount(String maKhang) {
        try {
            return jdbcTemplate.queryForObject("SELECT ma_dviqly FROM khach_hang WHERE ma_khang = ?", String.class, maKhang);
        } catch (Exception e) {
            return null;
        }
    }
}
