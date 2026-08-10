package com.evn.billing.worker.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Repository
public class SelfHealingRepositoryImpl implements SelfHealingRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void insertErrorLog(String maKhang, String month, int period, String errorType, String errorDetails) {
        String sql = "INSERT INTO nhat_ky_loi_tinh_toan (ma_khang, thang_chu_ky, ky_chot, loai_loi, chi_tiet_loi) VALUES (?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql, maKhang, month, period, errorType, errorDetails);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void insertDlqTask(String maKhang, String month, int period, int retryCount, String errorMessage, Timestamp nextRetryAt) {
        String sql = "INSERT INTO lich_xu_ly_lai (ma_khang, thang_chu_ky, ky_chot, so_lan_thu_lai, loi_cuoi_cung, thoi_gian_thu_lai_ke, trang_thai) " +
                "VALUES (?, ?, ?, ?, ?, ?, 'PENDING') " +
                "ON CONFLICT DO NOTHING";
        jdbcTemplate.update(sql, maKhang, month, period, retryCount, errorMessage, nextRetryAt);
    }

    @Override
    public List<Map<String, Object>> findPendingRetryTasks(int limit) {
        String sql = "SELECT * FROM lich_xu_ly_lai WHERE trang_thai = 'PENDING' AND thoi_gian_thu_lai_ke <= NOW() LIMIT ?";
        return jdbcTemplate.queryForList(sql, limit);
    }

    @Override
    public void markRetryTaskCompleted(Long taskId) {
        jdbcTemplate.update("UPDATE lich_xu_ly_lai SET trang_thai = 'COMPLETED' WHERE id_nhiem_vu = ?", taskId);
    }

    @Override
    public void markRetryTaskFailed(Long taskId, String note) {
        jdbcTemplate.update("UPDATE lich_xu_ly_lai SET trang_thai = 'FAILED', loi_cuoi_cung = ? WHERE id_nhiem_vu = ?", note, taskId);
    }

    @Override
    public String findBookFromBillingStatus(String maKhang, String month, int period) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT dtuong_qly FROM trang_thai_tinh_toan_kh WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? LIMIT 1",
                    String.class, maKhang, month, period);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String findBookByAccountId(String maKhang) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT dtuong_qly FROM diem_do WHERE ma_khang = ? LIMIT 1",
                    String.class, maKhang);
        } catch (Exception e) {
            return null;
        }
    }
}
