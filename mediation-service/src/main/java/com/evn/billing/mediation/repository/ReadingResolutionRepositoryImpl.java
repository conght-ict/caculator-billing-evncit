package com.evn.billing.mediation.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;

@Repository
public class ReadingResolutionRepositoryImpl implements ReadingResolutionRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public String findBookByAccountId(String maKhang) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT dtuong_qly FROM diem_do WHERE ma_khang = ? LIMIT 1", String.class, maKhang);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Optional<Map<String, Object>> findLatestActiveScheduleByBook(String dtuongQly) {
        try {
            Map<String, Object> schedule = jdbcTemplate.queryForMap(
                    "SELECT thang_ck, ky_chot FROM lich_ghi_dqly WHERE dtuong_qly = ? AND tthai_lich = 'ACTIVE' ORDER BY updated_at DESC LIMIT 1",
                    dtuongQly);
            return Optional.of(schedule);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public Long findSuspectOrPendingUsageId(String maKhang, String month, int period) {
        try {
            String sql = "SELECT id_chi_so FROM chi_so_dien_nang WHERE ma_khang = ? AND thang_chu_ky = ? " +
                    "AND ky_chot = ? AND lan_doc_phu = 1 AND trang_thai_xu_ly IN ('SUSPECT', 'PENDING_MANUAL') LIMIT 1";
            return jdbcTemplate.queryForObject(sql, Long.class, maKhang, month, period);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Long findAnyUsageId(String maKhang, String month, int period) {
        try {
            String sql = "SELECT id_chi_so FROM chi_so_dien_nang WHERE ma_khang = ? AND thang_chu_ky = ? " +
                    "AND ky_chot = ? AND lan_doc_phu = 1 LIMIT 1";
            return jdbcTemplate.queryForObject(sql, Long.class, maKhang, month, period);
        } catch (Exception e) {
            return null;
        }
    }
}
