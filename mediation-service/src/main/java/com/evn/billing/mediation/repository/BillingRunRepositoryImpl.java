package com.evn.billing.mediation.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class BillingRunRepositoryImpl implements BillingRunRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public int countActiveAccountsInBook(String dtuongQly) {
        Integer total = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT d.ma_khang) FROM diem_do d JOIN khach_hang k ON d.ma_khang = k.ma_khang WHERE d.dtuong_qly = ? AND k.trang_thai = 'ACTIVE'",
                Integer.class, dtuongQly);
        return total != null ? total : 0;
    }

    @Override
    public int countSuccessAccounts(String dtuongQly, String month, int period) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM trang_thai_tinh_toan_kh WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai = 'SUCCESS'",
                Integer.class, dtuongQly, month, period);
        return count != null ? count : 0;
    }

    @Override
    public void transitionSuccessToSuccessCmis(String dtuongQly, String month, int period) {
        jdbcTemplate.update(
                "UPDATE trang_thai_tinh_toan_kh SET trang_thai = 'SUCCESS_CMIS', updated_at = NOW() WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai = 'SUCCESS'",
                dtuongQly, month, period);
    }

    @Override
    public String findMaDviqlyByBook(String dtuongQly) {
        try {
            String value = jdbcTemplate.queryForObject(
                    "SELECT ma_dviqly FROM diem_do WHERE dtuong_qly = ? LIMIT 1", String.class, dtuongQly);
            return value != null ? value : "PD0600";
        } catch (Exception e) {
            return "PD0600";
        }
    }

    @Override
    public void upsertBookRunProcessing(String dtuongQly, String month, int period, int totalAccounts, int alreadyCalculated, String maDviqly) {
        String sql = "INSERT INTO lich_ghi_dqly (dtuong_qly, thang_ck, ky_chot, tu_ngay, den_ngay, tthai_lich, tthai_chay, tong_kh, kh_da_xl, kh_tc, kh_tb, nguon, ma_dviqly, created_at, updated_at) " +
                "VALUES (?, ?, ?, CURRENT_DATE, CURRENT_DATE, 'ACTIVE', 'PROCESSING', ?, ?, ?, 0, 'CMIS', ?, NOW(), NOW()) " +
                "ON CONFLICT (dtuong_qly, thang_ck, ky_chot) DO UPDATE SET " +
                "tthai_chay = 'PROCESSING', tong_kh = EXCLUDED.tong_kh, kh_da_xl = EXCLUDED.kh_da_xl, kh_tc = EXCLUDED.kh_tc, kh_tb = 0, ma_dviqly = EXCLUDED.ma_dviqly, updated_at = NOW()";
        jdbcTemplate.update(sql, dtuongQly, month, period, totalAccounts, alreadyCalculated, alreadyCalculated, maDviqly);
    }

    @Override
    public void transitionEligibleAccountsToProcessing(String dtuongQly, String month, int period) {
        String sql = "INSERT INTO trang_thai_tinh_toan_kh (ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, updated_at) " +
                "SELECT DISTINCT d.ma_khang, ?, d.dtuong_qly, ?, 'PROCESSING', NOW() " +
                "FROM diem_do d " +
                "JOIN khach_hang k ON d.ma_khang = k.ma_khang " +
                "WHERE d.dtuong_qly = ? AND k.trang_thai = 'ACTIVE' " +
                "ON CONFLICT (ma_khang, thang_chu_ky, ky_chot) DO UPDATE " +
                "SET trang_thai = 'PROCESSING', thong_bao_loi = NULL, updated_at = NOW() " +
                "WHERE trang_thai_tinh_toan_kh.trang_thai IN ('PENDING', 'FAILED', 'INCOMPLETE', 'CANCELLED', 'SUSPECT', 'WARNING')";
        jdbcTemplate.update(sql, month, period, dtuongQly);
    }

    @Override
    public void updateBookRunFinalStatus(String dtuongQly, String month, int period, String scheduleRunStatus) {
        String sql = "UPDATE lich_ghi_dqly SET tthai_chay = ?, updated_at = NOW() WHERE dtuong_qly = ? AND thang_ck = ? AND ky_chot = ?";
        jdbcTemplate.update(sql, scheduleRunStatus, dtuongQly, month, period);
    }

    @Override
    public boolean isSnapshotGenerated(String dtuongQly, String month, int period) {
        try {
            Boolean value = jdbcTemplate.queryForObject(
                    "SELECT snapshot_generated FROM lich_ghi_dqly WHERE dtuong_qly = ? AND thang_ck = ? AND ky_chot = ?",
                    Boolean.class, dtuongQly, month, period);
            return value != null && value;
        } catch (Exception e) {
            return false;
        }
    }
}
