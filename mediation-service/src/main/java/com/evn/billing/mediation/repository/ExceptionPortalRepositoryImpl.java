package com.evn.billing.mediation.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Repository
public class ExceptionPortalRepositoryImpl implements ExceptionPortalRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private AmrIngestionRepository amrIngestionRepository;

    @Override
    public List<Map<String, Object>> findPendingExceptions(String dtuongQly, String month, int period) {
        String sql = "SELECT c.id_chi_so, c.ma_khang, c.ma_ddo, c.tu_ngay, c.den_ngay, " +
                "c.chi_so_dau, c.chi_so_cuoi, c.san_luong_tho, c.trang_thai_xu_ly, c.tgian_bdien, " +
                "t.thong_bao_loi " +
                "FROM chi_so_dien_nang c " +
                "JOIN trang_thai_tinh_toan_kh t ON c.ma_khang = t.ma_khang " +
                "AND c.thang_chu_ky = t.thang_chu_ky AND c.ky_chot = t.ky_chot " +
                "WHERE t.dtuong_qly = ? AND c.thang_chu_ky = ? AND c.ky_chot = ? " +
                "AND c.trang_thai_xu_ly = 'PENDING_MANUAL'";
        return jdbcTemplate.queryForList(sql, dtuongQly, month, period);
    }

    @Override
    @Transactional
    public void resolveException(Long usageId, String month, BigDecimal correctedEndIndex, String operatorNote) {
        // 1. Get old record details
        String selectSql = "SELECT * FROM chi_so_dien_nang WHERE id_chi_so = ? AND thang_chu_ky = ?";
        Map<String, Object> oldRecord = jdbcTemplate.queryForMap(selectSql, usageId, month);

        String maKhang = (String) oldRecord.get("ma_khang");
        String meterPointId = (String) oldRecord.get("ma_ddo");
        int period = ((Number) oldRecord.get("ky_chot")).intValue();
        BigDecimal startIndex = (BigDecimal) oldRecord.get("chi_so_dau");
        boolean isRollover = (Boolean) oldRecord.get("co_quay_vong");
        int oldSubSeq = ((Number) oldRecord.get("lan_doc_phu")).intValue();
        String timePeriod = (String) oldRecord.get("tgian_bdien");

        // 2. Mark old record as REPLACED
        String updateOldSql = "UPDATE chi_so_dien_nang SET trang_thai_xu_ly = 'REPLACED' WHERE id_chi_so = ? AND thang_chu_ky = ?";
        jdbcTemplate.update(updateOldSql, usageId, month);

        // 3. Compute new raw consumption: End_Index - Start_Index
        BigDecimal rawConsumption = correctedEndIndex.subtract(startIndex);
        if (rawConsumption.compareTo(BigDecimal.ZERO) < 0) {
            rawConsumption = BigDecimal.ZERO; // Fallback or handle rollover if needed
        }

        // 4. Insert new CORRECTION record with incremented subReadingSeq (lan_doc_phu)
        String insertNewSql = "INSERT INTO chi_so_dien_nang (" +
                "lan_doc_phu, ma_khang, ma_ddo, thang_chu_ky, ky_chot, tu_ngay, den_ngay, " +
                "chi_so_dau, chi_so_cuoi, co_quay_vong, san_luong_tho, trang_thai_xu_ly, " +
                "loai_ghi_index, nguon_ghi, tgian_bdien" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'VALIDATED', 'CORRECTION', 'CMIS_RESOLVE', ?)";
        
        jdbcTemplate.update(insertNewSql,
                oldSubSeq + 1,
                maKhang,
                meterPointId,
                month,
                period,
                oldRecord.get("tu_ngay"),
                oldRecord.get("den_ngay"),
                startIndex,
                correctedEndIndex,
                isRollover,
                rawConsumption,
                timePeriod
        );

        // 5. Update customer billing status back to PROCESSING to trigger billing flow
        String updateStatusSql = "UPDATE trang_thai_tinh_toan_kh SET trang_thai = 'PROCESSING', thong_bao_loi = NULL, updated_at = NOW() " +
                "WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ?";
        jdbcTemplate.update(updateStatusSql, maKhang, month, period);
    }

    @Override
    public int countPendingExceptions(String dtuongQly, String month, int period) {
        String sql = "SELECT COUNT(*) FROM chi_so_dien_nang c " +
                "JOIN trang_thai_tinh_toan_kh t ON c.ma_khang = t.ma_khang " +
                "AND c.thang_chu_ky = t.thang_chu_ky AND c.ky_chot = t.ky_chot " +
                "WHERE t.dtuong_qly = ? AND c.thang_chu_ky = ? AND c.ky_chot = ? " +
                "AND c.trang_thai_xu_ly = 'PENDING_MANUAL'";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, dtuongQly, month, period);
        return count != null ? count : 0;
    }

    @Override
    public Map<String, Object> findAccountAndPeriodByUsageId(Long usageId, String month) {
        String selectSql = "SELECT ma_khang, ky_chot FROM chi_so_dien_nang WHERE id_chi_so = ? AND thang_chu_ky = ?";
        return jdbcTemplate.queryForMap(selectSql, usageId, month);
    }
}
