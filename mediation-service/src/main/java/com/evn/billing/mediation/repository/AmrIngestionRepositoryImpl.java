package com.evn.billing.mediation.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@Repository
public class AmrIngestionRepositoryImpl implements AmrIngestionRepository {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AmrIngestionRepositoryImpl.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void migrateSchema() {
        jdbcTemplate.execute("ALTER TABLE chi_so_dien_nang ADD COLUMN IF NOT EXISTS ma_cto VARCHAR(50)");
        jdbcTemplate.execute("ALTER TABLE chi_so_dien_nang ADD COLUMN IF NOT EXISTS so_lan_quay_vong INT NOT NULL DEFAULT 1");
    }



    @Override
    public List<Map<String, Object>> findActivePendingSchedules() {
        return jdbcTemplate.queryForList(
                "SELECT dtuong_qly, thang_ck, ky_chot, den_ngay, n_tru, n_cong FROM lich_ghi_dqly " +
                "WHERE tthai_lich = 'ACTIVE' AND tthai_chay IN ('PENDING', 'WAITING_AMR')"
        );
    }

    @Override
    public List<Map<String, Object>> findActiveMetersByDtuongQly(String dtuongQly) {
        return jdbcTemplate.queryForList(
                "SELECT ma_ddo, ma_khang, loai_ddo, CAST(thong_tin_cto AS TEXT) AS thong_tin_cto FROM diem_do WHERE dtuong_qly = ? AND trang_thai = 'ACTIVE'",
                dtuongQly
        );
    }

    @Override
    public List<Map<String, Object>> findIngestedReadings(String month, int period) {
        return jdbcTemplate.queryForList(
                "SELECT ma_ddo, tgian_bdien, ma_cto FROM chi_so_dien_nang WHERE thang_chu_ky = ? AND ky_chot = ? AND trang_thai_xu_ly IN ('VALIDATED', 'PENDING_MANUAL')",
                month, period
        );
    }

    @Override
    public List<Map<String, Object>> findOracleReadings(List<String> meterIds, java.time.LocalDateTime start, java.time.LocalDateTime end) {
        if (meterIds == null || meterIds.isEmpty()) return java.util.Collections.emptyList();
        String placeholders = meterIds.stream().map(m -> "?").collect(java.util.stream.Collectors.joining(","));
        String querySql = "SELECT ma_ddo, bcs, chi_so_dau, chi_so_cuoi, ngay_doc, co_quay_vong, san_luong " +
                "FROM oracle_amr_data " +
                "WHERE ma_ddo IN (" + placeholders + ") " +
                "AND ngay_doc >= ? AND ngay_doc <= ?";
        java.util.List<Object> params = new java.util.ArrayList<>(meterIds);
        params.add(java.sql.Timestamp.valueOf(start));
        params.add(java.sql.Timestamp.valueOf(end));
        return jdbcTemplate.queryForList(querySql, params.toArray());
    }

    @Override
    public void updateScheduleStatus(String dtuongQly, String month, int period, String status) {
        jdbcTemplate.update(
                "UPDATE lich_ghi_dqly SET tthai_chay = ?, updated_at = NOW() " +
                "WHERE dtuong_qly = ? AND thang_ck = ? AND ky_chot = ?",
                status, dtuongQly, month, period
        );
    }

    @Override
    public void batchInsertAmrReadings(List<Object[]> params) {
        String insertSql = "INSERT INTO chi_so_dien_nang (" +
            "id_chi_so, lan_doc_phu, ma_khang, ma_ddo, thang_chu_ky, ky_chot, " +
            "tu_ngay, den_ngay, chi_so_dau, chi_so_cuoi, co_quay_vong, " +
            "san_luong_tho, trang_thai_xu_ly, loai_ghi_index, nguon_ghi, tgian_bdien, ma_cto) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ORIGINAL', 'AMR', ?, ?) " +
            "ON CONFLICT DO NOTHING";
        jdbcTemplate.batchUpdate(insertSql, params);
    }

    @Override
    public String findDtuongQlyByAccountId(String accountId) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT dtuong_qly FROM diem_do WHERE ma_khang = ? LIMIT 1", String.class, accountId
            );
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> findScheduleTolerance(String dtuongQly, String month, int period) {
        return jdbcTemplate.queryForList(
            "SELECT den_ngay, n_tru, n_cong FROM lich_ghi_dqly WHERE dtuong_qly = ? AND thang_ck = ? AND ky_chot = ?",
            dtuongQly, month, period
        );
    }

    @Override
    public void batchInsertCmisReadings(List<Object[]> params) {
        String sql = "INSERT INTO chi_so_dien_nang (" +
                "id_chi_so, lan_doc_phu, ma_khang, ma_ddo, thang_chu_ky, ky_chot, " +
                "tu_ngay, den_ngay, chi_so_dau, chi_so_cuoi, co_quay_vong, " +
                "san_luong_tho, trang_thai_xu_ly, loai_ghi_index, nguon_ghi, tgian_bdien, ma_cto, so_lan_quay_vong) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ORIGINAL', ?, ?, ?, ?) " +
                "ON CONFLICT DO NOTHING";
        jdbcTemplate.batchUpdate(sql, params);
    }

    @Override
    public boolean tryAcquireBillingTriggerGate(String dtuongQly, String accountId, String month, int period) {
        try {
            String sql = "INSERT INTO trang_thai_tinh_toan_kh " +
                    "(ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, updated_at) " +
                    "VALUES (?, ?, ?, ?, 'PROCESSING', NOW()) " +
                    "ON CONFLICT (ma_khang, thang_chu_ky, ky_chot) DO UPDATE " +
                    "SET trang_thai = 'PROCESSING', dtuong_qly = EXCLUDED.dtuong_qly, thong_bao_loi = NULL, updated_at = NOW() " +
                    "WHERE trang_thai_tinh_toan_kh.trang_thai IN ('FAILED', 'INCOMPLETE', 'PENDING_MANUAL', 'SUSPECT', 'WARNING', 'CANCELLED', 'PENDING')";
            int rows = jdbcTemplate.update(sql, accountId, month, dtuongQly, period);
            return rows > 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void logIncompleteStatus(String accountId, String month, String dtuongQly, int period, String missingStr) {
        String updateSql = "INSERT INTO trang_thai_tinh_toan_kh " +
                "(ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, thong_bao_loi, updated_at) " +
                "VALUES (?, ?, ?, ?, 'INCOMPLETE', ?, NOW()) " +
                "ON CONFLICT (ma_khang, thang_chu_ky, ky_chot) DO UPDATE SET " +
                "trang_thai = 'INCOMPLETE', dtuong_qly = EXCLUDED.dtuong_qly, thong_bao_loi = EXCLUDED.thong_bao_loi, updated_at = NOW()";
        jdbcTemplate.update(updateSql, accountId, month, dtuongQly, period, missingStr);
    }

    @Override
    public List<Map<String, Object>> getKh2tpPmaxStatus(String accountId, String month, int period) {
        List<String> metersRequiringPmax = jdbcTemplate.queryForList(
            "SELECT ma_ddo FROM diem_do WHERE ma_khang = ? AND EXISTS (SELECT 1 FROM jsonb_array_elements(thong_tin_cto) elem WHERE elem->'danh_sach_bcs' ? 'PMAX')",
            String.class, accountId
        );
        if (metersRequiringPmax.isEmpty()) {
            return java.util.Collections.emptyList();
        }

        List<Map<String, Object>> stackingRelation = jdbcTemplate.queryForList(
            "SELECT id_quan_he FROM quan_he_diem_do WHERE ma_ddo_cha IN (SELECT ma_ddo FROM diem_do WHERE ma_khang = ?) " +
            "OR ma_ddo_con IN (SELECT ma_ddo FROM diem_do WHERE ma_khang = ?)",
            accountId, accountId
        );

        if (!stackingRelation.isEmpty()) {
            Integer pmaxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*)::int FROM chi_so_dien_nang WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? " +
                "AND tgian_bdien = 'PMAX' AND trang_thai_xu_ly IN ('VALIDATED', 'PENDING_MANUAL')",
                Integer.class, accountId, month, period
            );
            if (pmaxCount == null || pmaxCount == 0) {
                Map<String, Object> err = new HashMap<>();
                err.put("ma_ddo", "STACKING_GROUP");
                err.put("error", "Missing Pmax for stacking group of customer: " + accountId);
                return java.util.Collections.singletonList(err);
            }
            return java.util.Collections.emptyList();
        } else {
            return jdbcTemplate.queryForList(
                "SELECT d.ma_ddo FROM diem_do d WHERE d.ma_khang = ? AND EXISTS (SELECT 1 FROM jsonb_array_elements(d.thong_tin_cto) elem WHERE elem->'danh_sach_bcs' ? 'PMAX') " +
                "AND NOT EXISTS (SELECT 1 FROM chi_so_dien_nang c WHERE c.ma_ddo = d.ma_ddo " +
                "AND c.thang_chu_ky = ? AND c.ky_chot = ? AND c.tgian_bdien = 'PMAX' AND c.trang_thai_xu_ly IN ('VALIDATED', 'PENDING_MANUAL'))",
                accountId, month, period
            );
        }
    }

    @Override
    public List<Map<String, Object>> getReactivePowerStatus(String accountId, String month, int period) {
        String sql = "SELECT ma_ddo, " +
                "SUM(CASE WHEN tgian_bdien = 'VC' THEN san_luong_tho ELSE 0 END) as sum_vc, " +
                "SUM(CASE WHEN tgian_bdien IN ('BT', 'CD', 'TD', 'KT') THEN san_luong_tho ELSE 0 END) as sum_hc " +
                "FROM chi_so_dien_nang " +
                "WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai_xu_ly IN ('VALIDATED', 'PENDING_MANUAL') " +
                "GROUP BY ma_ddo " +
                "HAVING SUM(CASE WHEN tgian_bdien = 'VC' THEN san_luong_tho ELSE 0 END) > 0 " +
                "AND SUM(CASE WHEN tgian_bdien IN ('BT', 'CD', 'TD', 'KT') THEN san_luong_tho ELSE 0 END) <= 0";
        return jdbcTemplate.queryForList(sql, accountId, month, period);
    }

    @Override
    public java.math.BigDecimal getPreviousPeriodConsumption(String accountId, String currentMonth, int currentPeriod) {
        List<Map<String, Object>> latestPeriod = jdbcTemplate.queryForList(
                "SELECT thang_chu_ky, ky_chot FROM chi_so_dien_nang " +
                "WHERE ma_khang = ? AND (thang_chu_ky < ? OR (thang_chu_ky = ? AND ky_chot < ?)) " +
                "AND trang_thai_xu_ly IN ('VALIDATED', 'PENDING_MANUAL') " +
                "ORDER BY thang_chu_ky DESC, ky_chot DESC LIMIT 1",
                accountId, currentMonth, currentMonth, currentPeriod
        );
        if (latestPeriod.isEmpty()) {
            return java.math.BigDecimal.ZERO;
        }
        String prevMonth = (String) latestPeriod.get(0).get("thang_chu_ky");
        int prevPeriod = ((Number) latestPeriod.get(0).get("ky_chot")).intValue();
        
        java.math.BigDecimal sum = jdbcTemplate.queryForObject(
                "SELECT SUM(san_luong_tho) FROM chi_so_dien_nang " +
                "WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai_xu_ly IN ('VALIDATED', 'PENDING_MANUAL')",
                java.math.BigDecimal.class, accountId, prevMonth, prevPeriod
        );
        return sum != null ? sum : java.math.BigDecimal.ZERO;
    }

    @Override
    public List<java.math.BigDecimal> getHistoricalConsumptions(String accountId, String currentMonth, int currentPeriod) {
        String sql = "SELECT SUM(san_luong_tho) as consumption " +
                "FROM chi_so_dien_nang " +
                "WHERE ma_khang = ? AND ( " +
                "    (thang_chu_ky < ?) OR " +
                "    (thang_chu_ky = ? AND ky_chot < ?) " +
                ") AND trang_thai_xu_ly IN ('VALIDATED', 'PENDING_MANUAL') " +
                "GROUP BY thang_chu_ky, ky_chot " +
                "ORDER BY thang_chu_ky DESC, ky_chot DESC " +
                "LIMIT 12";
        List<java.math.BigDecimal> list = jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.math.BigDecimal val = rs.getBigDecimal("consumption");
            return val != null ? val : java.math.BigDecimal.ZERO;
        }, accountId, currentMonth, currentMonth, currentPeriod);
        
        java.util.Collections.reverse(list);
        return list;
    }

    @Override
    public void updateCustomerBillingStatus(String accountId, String month, int period, String status, String errorMsg) {
        String sql = "INSERT INTO trang_thai_tinh_toan_kh " +
                "(ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, thong_bao_loi, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, NOW()) " +
                "ON CONFLICT (ma_khang, thang_chu_ky, ky_chot) DO UPDATE SET " +
                "trang_thai = EXCLUDED.trang_thai, thong_bao_loi = EXCLUDED.thong_bao_loi, updated_at = NOW()";
        String dtuongQly = findDtuongQlyByAccountId(accountId);
        if (dtuongQly == null) dtuongQly = "UNKNOWN";
        
        jdbcTemplate.update(sql, accountId, month, dtuongQly, period, status, errorMsg);
    }
 
    @Override
    public boolean isBatchJobRunning(String dtuongQly, String month, int period) {
        try {
            String sql = "SELECT tthai_chay FROM lich_ghi_dqly WHERE dtuong_qly = ? AND thang_ck = ? AND ky_chot = ? LIMIT 1";
            String runStatus = jdbcTemplate.queryForObject(sql, String.class, dtuongQly, month, period);
            return "PROCESSING".equalsIgnoreCase(runStatus);
        } catch (Exception e) {
            return false;
        }
    }
 
    @Override
    public void logIngestionLifecycle(String accountId, String meterPointId, String month, Integer period, String step, String status, String detailJson, String source) {
        try {
            String sql = "INSERT INTO nhat_ky_chi_so (ma_khang, ma_ddo, thang_chu_ky, ky_chot, buoc_xu_ly, trang_thai, chi_tiet, nguon_ghi, created_at) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, NOW())";
            jdbcTemplate.update(sql, accountId, meterPointId, month, period, step, status, detailJson, source);
        } catch (Exception e) {
            log.error("Failed to write ingestion lifecycle log for Account: {}, error: {}", accountId, e.getMessage());
        }
    }
}
