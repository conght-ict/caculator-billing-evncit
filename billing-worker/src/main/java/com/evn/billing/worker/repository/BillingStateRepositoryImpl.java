package com.evn.billing.worker.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Repository
public class BillingStateRepositoryImpl implements BillingStateRepository {

    private static final Logger log = LoggerFactory.getLogger(BillingStateRepositoryImpl.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int tryClaimProcessingWorker(String workerNodeId, String maKhang, String month, int period, int claimTimeoutMinutes) {
        String sql = "UPDATE trang_thai_tinh_toan_kh SET ten_worker = ?, updated_at = NOW() " +
                "WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai = 'PROCESSING' " +
                "AND (ten_worker IS NULL OR updated_at < NOW() - (? * INTERVAL '1 minute'))";
        return jdbcTemplate.update(sql, workerNodeId, maKhang, month, period, claimTimeoutMinutes);
    }

    @Override
    public void seedProcessingStatus(String maKhang, String month, String dtuongQly, int period, String workerNode) {
        String seedSql = "INSERT INTO trang_thai_tinh_toan_kh " +
                "(ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, ten_worker, updated_at) " +
                "VALUES (?, ?, ?, ?, 'PROCESSING', ?, NOW()) " +
                "ON CONFLICT (ma_khang, thang_chu_ky, ky_chot) DO NOTHING";
        jdbcTemplate.update(seedSql, maKhang, month, dtuongQly, period, workerNode);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int updateProcessingStatus(String status, String invoiceId, String errorMsg, Long durationMs, String workerNode, String dtuongQly, String maKhang, String month, int period) {
        String updateSql = "UPDATE trang_thai_tinh_toan_kh SET " +
                "trang_thai = ?, id_hoa_don = ?, thong_bao_loi = ?, thoi_gian_xu_ly_ms = ?, ten_worker = ?, dtuong_qly = ?, updated_at = NOW() " +
                "WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai = 'PROCESSING' " +
                "AND (ten_worker = ? OR ten_worker IS NULL)";
        return jdbcTemplate.update(
                updateSql,
                status,
                invoiceId,
                errorMsg,
                durationMs,
                workerNode,
                dtuongQly,
                maKhang,
                month,
                period,
                workerNode
        );
    }

    @Override
    public void updateBookBillingRunProgress(String dtuongQly, String month, int period, int processedDelta, int successDelta, int failedDelta) {
        String sql = "UPDATE lich_ghi_dqly SET " +
                "kh_da_xl = GREATEST(0, kh_da_xl + ?), " +
                "kh_tc = GREATEST(0, kh_tc + ?), " +
                "kh_tb = GREATEST(0, kh_tb + ?), " +
                "updated_at = NOW() " +
                "WHERE dtuong_qly = ? AND thang_ck = ? AND ky_chot = ?";
        jdbcTemplate.update(sql, processedDelta, successDelta, failedDelta, dtuongQly, month, period);
    }

    @Override
    public void upsertInvoice(String invoiceId, String maKhang, String dtuongQly, String month, BigDecimal totalBeforeTax, BigDecimal taxAmount, BigDecimal totalAfterTax, String idempotencyKey, String manifestJson, boolean isProrated, String refSnapshot, String status, String maDviqly, Timestamp createdAt, Timestamp updatedAt) {
        String insertInvoiceSql = "INSERT INTO hoa_don (" +
                "id_hoa_don, ma_khang, dtuong_qly, thang_chu_ky, " +
                "tong_tien_truoc_thue, tien_thue, tong_tien_sau_thue, " +
                "khoa_lap_trung, ban_ke_tinh_toan, ap_dung_phan_bo, " +
                "ref_snapshot, trang_thai_tinh_toan, ma_dviqly, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (khoa_lap_trung, thang_chu_ky) " +
                "DO UPDATE SET " +
                "tong_tien_truoc_thue = EXCLUDED.tong_tien_truoc_thue, " +
                "tien_thue            = EXCLUDED.tien_thue, " +
                "tong_tien_sau_thue   = EXCLUDED.tong_tien_sau_thue, " +
                "ban_ke_tinh_toan      = EXCLUDED.ban_ke_tinh_toan, " +
                "ap_dung_phan_bo     = EXCLUDED.ap_dung_phan_bo, " +
                "ref_snapshot          = EXCLUDED.ref_snapshot, " +
                "trang_thai_tinh_toan  = EXCLUDED.trang_thai_tinh_toan, " +
                "ma_dviqly            = EXCLUDED.ma_dviqly, " +
                "updated_at            = NOW()";

        jdbcTemplate.update(
                insertInvoiceSql,
                invoiceId, maKhang, dtuongQly, month,
                totalBeforeTax, taxAmount, totalAfterTax,
                idempotencyKey, manifestJson, isProrated,
                refSnapshot, status, maDviqly, createdAt, updatedAt
        );
    }

    @Override
    public void lockSnapshot(String maKhang, String month, int period, int version) {
        String lockSnapshotSql = "UPDATE snapshot_tinh_toan SET trang_thai = 'LOCKED' " +
                "WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND phien_ban_tinh = ?";
        jdbcTemplate.update(lockSnapshotSql, maKhang, month, period, version);
    }

    @Override
    public void insertOutboxEvent(UUID eventId, String aggregateType, String aggregateId, String eventType, String payloadJson, Timestamp createdAt) {
        String insertOutboxSql = "INSERT INTO su_kien_outbox (id_su_kien, loai_doi_tuong, id_doi_tuong, loai_su_kien, noi_dung, trang_thai, created_at) " +
                "VALUES (?, ?, ?, ?, ?::jsonb, 'PENDING', ?) " +
                "ON CONFLICT (loai_doi_tuong, id_doi_tuong, loai_su_kien) DO NOTHING";
        jdbcTemplate.update(insertOutboxSql, eventId, aggregateType, aggregateId, eventType, payloadJson, createdAt);
    }

    @Override
    public void batchUpsertInvoices(List<Object[]> invoiceBatch) {
        String insertInvoiceSql = "INSERT INTO hoa_don (" +
                "id_hoa_don, ma_khang, dtuong_qly, thang_chu_ky, ky_chot, " +
                "tong_tien_truoc_thue, tien_thue, tong_tien_sau_thue, " +
                "khoa_lap_trung, ban_ke_tinh_toan, ap_dung_phan_bo, " +
                "ref_snapshot, trang_thai_tinh_toan, ma_dviqly, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (khoa_lap_trung, thang_chu_ky) " +
                "DO UPDATE SET " +
                "tong_tien_truoc_thue = EXCLUDED.tong_tien_truoc_thue, " +
                "tien_thue            = EXCLUDED.tien_thue, " +
                "tong_tien_sau_thue   = EXCLUDED.tong_tien_sau_thue, " +
                "ban_ke_tinh_toan      = EXCLUDED.ban_ke_tinh_toan, " +
                "ap_dung_phan_bo     = EXCLUDED.ap_dung_phan_bo, " +
                "ref_snapshot          = EXCLUDED.ref_snapshot, " +
                "trang_thai_tinh_toan  = EXCLUDED.trang_thai_tinh_toan, " +
                "ma_dviqly            = EXCLUDED.ma_dviqly, " +
                "updated_at            = NOW()";
        jdbcTemplate.batchUpdate(insertInvoiceSql, invoiceBatch);
    }

    @Override
    public void batchInsertOutbox(List<Object[]> outboxBatch) {
        String insertOutboxSql = "INSERT INTO su_kien_outbox (id_su_kien, loai_doi_tuong, id_doi_tuong, loai_su_kien, noi_dung, trang_thai, created_at) " +
                "VALUES (?, ?, ?, ?, ?::jsonb, 'PENDING', ?) " +
                "ON CONFLICT (loai_doi_tuong, id_doi_tuong, loai_su_kien) DO NOTHING";
        jdbcTemplate.batchUpdate(insertOutboxSql, outboxBatch);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void batchUpsertStatuses(List<Object[]> statusBatch) {
        String insertStatusSql = "INSERT INTO trang_thai_tinh_toan_kh (ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, id_hoa_don, thong_bao_loi, ten_worker, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (ma_khang, thang_chu_ky, ky_chot) DO UPDATE SET " +
                "trang_thai = EXCLUDED.trang_thai, id_hoa_don = EXCLUDED.id_hoa_don, thong_bao_loi = EXCLUDED.thong_bao_loi, ten_worker = EXCLUDED.ten_worker, dtuong_qly = EXCLUDED.dtuong_qly, updated_at = EXCLUDED.updated_at";
        jdbcTemplate.batchUpdate(insertStatusSql, statusBatch);
    }

    @Override
    public List<String> findParentAccountIds(String childAccountId) {
        String sql = "SELECT DISTINCT parent.ma_khang " +
                "FROM diem_do child " +
                "JOIN quan_he_diem_do qh ON child.ma_ddo = qh.ma_ddo_con " +
                "JOIN diem_do parent ON qh.ma_ddo_cha = parent.ma_ddo " +
                "WHERE child.ma_khang = ?";
        try {
            return jdbcTemplate.queryForList(sql, String.class, childAccountId);
        } catch (Exception e) {
            log.error("[SQL-ERROR] Failed to query parent account IDs for child '{}': {}", childAccountId, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public Map<String, Object> findStatusRowForUpdate(String maKhang, String month, int period) {
        String sql = "SELECT dtuong_qly, trang_thai FROM trang_thai_tinh_toan_kh WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? FOR UPDATE";
        return jdbcTemplate.queryForMap(sql, maKhang, month, period);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateAccountStatus(String targetStatus, String maKhang, String month, int period) {
        if ("PROCESSING".equals(targetStatus)) {
            jdbcTemplate.update(
                    "UPDATE trang_thai_tinh_toan_kh SET trang_thai = ?, ten_worker = NULL, thong_bao_loi = NULL, updated_at = NOW() WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ?",
                    targetStatus, maKhang, month, period
            );
        } else {
            jdbcTemplate.update(
                    "UPDATE trang_thai_tinh_toan_kh SET trang_thai = ?, thong_bao_loi = NULL, updated_at = NOW() WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ?",
                    targetStatus, maKhang, month, period
            );
        }
    }

    @Override
    public void markInvoicesCancelled(String maKhang, String month, int period) {
        int rows = jdbcTemplate.update(
                "UPDATE hoa_don SET trang_thai_tinh_toan = 'CANCELLED', updated_at = NOW() WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai_tinh_toan != 'CANCELLED'",
                maKhang, month, period
        );
        log.info("[SQL-CANCEL-INVOICE] Updated {} rows in 'hoa_don' for maKhang={}, month={}, period={}", rows, maKhang, month, period);
    }

    @Override
    public void setSnapshotsDraft(String maKhang, String month, int period) {
        int rows = jdbcTemplate.update(
                "UPDATE snapshot_tinh_toan SET trang_thai = 'DRAFT' WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ?",
                maKhang, month, period
        );
        log.info("[SQL-CANCEL-SNAPSHOT] Updated {} rows in 'snapshot_tinh_toan' for maKhang={}, month={}, period={}", rows, maKhang, month, period);
    }

    @Override
    public void markAccountCancelled(String maKhang, String month, int period, String message) {
        int rows = jdbcTemplate.update(
                "UPDATE trang_thai_tinh_toan_kh SET trang_thai = 'CANCELLED', id_hoa_don = NULL, thong_bao_loi = ?, updated_at = NOW() WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ?",
                message, maKhang, month, period
        );
        log.info("[SQL-CANCEL-ACCOUNT] Updated {} rows in 'trang_thai_tinh_toan_kh' for maKhang={}, month={}, period={}", rows, maKhang, month, period);
    }

    @Override
    public void insertCancelAuditLog(String maKhang, String month, int period,
                                      String trangThaiCu, String nguoiHuy,
                                      String lyDoHuy, String nguonHuy) {
        jdbcTemplate.update(
            "INSERT INTO nhat_ky_huy_tinh " +
            "(ma_khang, thang_chu_ky, ky_chot, trang_thai_cu, nguoi_huy, ly_do_huy, nguon_huy) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            maKhang, month, period, trangThaiCu, nguoiHuy, lyDoHuy, nguonHuy
        );
    }

    @Override
    public int countValidatedReadings(String dtuongQly, String month, int period) {
        Integer value = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM trang_thai_tinh_toan_kh WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai NOT IN ('PENDING', 'READING', 'INCOMPLETE')",
                Integer.class, dtuongQly, month, period);
        return value != null ? value : 0;
    }

    @Override
    public int countByStatuses(String dtuongQly, String month, int period, List<String> statuses) {
        if (statuses == null || statuses.isEmpty()) {
            return 0;
        }
        String placeholders = statuses.stream().map(s -> "?").collect(Collectors.joining(", "));
        String sql = "SELECT COUNT(*) FROM trang_thai_tinh_toan_kh WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai IN (" + placeholders + ")";
        List<Object> params = new ArrayList<>();
        params.add(dtuongQly);
        params.add(month);
        params.add(period);
        params.addAll(statuses);
        Integer value = jdbcTemplate.queryForObject(sql, Integer.class, params.toArray());
        return value != null ? value : 0;
    }

    @Override
    public List<String> findLockableAccountsForBook(String dtuongQly, String month, int period) {
        return jdbcTemplate.queryForList(
                "SELECT ma_khang FROM trang_thai_tinh_toan_kh WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai IN ('SUCCESS', 'SUCCESS_CMIS', 'ANOMALY') FOR UPDATE",
                String.class, dtuongQly, month, period);
    }

    @Override
    public List<String> findCancelableAccountsForBook(String dtuongQly, String month, int period) {
        return jdbcTemplate.queryForList(
                "SELECT ma_khang FROM trang_thai_tinh_toan_kh WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai NOT IN ('LOCKED', 'E_INVOICE_ISSUED', 'CANCELLED') FOR UPDATE",
                String.class, dtuongQly, month, period);
    }

    @Override
    public void lockBookAccounts(String dtuongQly, String month, int period, String targetStatus) {
        jdbcTemplate.update(
                "UPDATE trang_thai_tinh_toan_kh SET trang_thai = ?, updated_at = NOW() WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai IN ('SUCCESS', 'SUCCESS_CMIS', 'ANOMALY')",
                targetStatus, dtuongQly, month, period
        );
    }

    @Override
    public Integer countTotalAccounts(String dtuongQly, String month, int period) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM trang_thai_tinh_toan_kh WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ?",
                Integer.class, dtuongQly, month, period);
    }

    @Override
    public Integer countSuccessfulForAutoBatch(String dtuongQly, String month, int period) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM trang_thai_tinh_toan_kh WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? " +
                        "AND trang_thai IN ('SUCCESS', 'SUCCESS_CMIS', 'ANOMALY', 'LOCKED', 'E_INVOICE_ISSUED', 'APPROVED_CMIS')",
                Integer.class, dtuongQly, month, period);
    }

    @Override
    public Integer findAutoBatchThreshold() {
        try {
            String raw = jdbcTemplate.queryForObject(
                    "SELECT gia_tri FROM cau_hinh_he_thong WHERE khoa_cau_hinh = 'auto_batch_threshold' LIMIT 1",
                    String.class);
            if (raw == null) {
                return null;
            }
            return Integer.parseInt(raw);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String findBookRunStatus(String dtuongQly, String month, int period) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT tthai_chay FROM lich_ghi_dqly WHERE dtuong_qly = ? AND thang_ck = ? AND ky_chot = ? LIMIT 1",
                    String.class, dtuongQly, month, period);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void approveBookAll(String dtuongQly, String month, int period) {
        jdbcTemplate.update(
                "UPDATE trang_thai_tinh_toan_kh SET trang_thai = 'APPROVED_CMIS', thong_bao_loi = NULL, updated_at = NOW() " +
                        "WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai IN ('SUCCESS', 'SUCCESS_CMIS', 'ANOMALY')",
                dtuongQly, month, period
        );
    }

    @Override
    public void approveBookExcluding(String dtuongQly, String month, int period, List<String> excludedAccounts) {
        if (excludedAccounts == null || excludedAccounts.isEmpty()) {
            approveBookAll(dtuongQly, month, period);
            return;
        }

        StringBuilder sql = new StringBuilder(
                "UPDATE trang_thai_tinh_toan_kh SET trang_thai = 'APPROVED_CMIS', thong_bao_loi = NULL, updated_at = NOW() " +
                        "WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai IN ('SUCCESS', 'SUCCESS_CMIS', 'ANOMALY')");
        List<Object> args = new ArrayList<>(List.of(dtuongQly, month, period));
        sql.append(" AND ma_khang NOT IN (");
        for (int i = 0; i < excludedAccounts.size(); i++) {
            sql.append(i == 0 ? "?" : ", ?");
            args.add(excludedAccounts.get(i));
        }
        sql.append(")");
        jdbcTemplate.update(sql.toString(), args.toArray());
    }

    @Override
    public void rejectAccountByCmis(String maKhang, String month, int period, String message) {
        jdbcTemplate.update(
                "UPDATE trang_thai_tinh_toan_kh SET trang_thai = 'REJECTED_CMIS', thong_bao_loi = ?, updated_at = NOW() " +
                        "WHERE ma_khang = ? AND thang_chu_ky = ? AND ky_chot = ?",
                message, maKhang, month, period
        );
    }

    @Override
    public List<String> findApprovedAccounts(String dtuongQly, String month, int period) {
        return jdbcTemplate.queryForList(
                "SELECT ma_khang FROM trang_thai_tinh_toan_kh WHERE dtuong_qly = ? AND thang_chu_ky = ? AND ky_chot = ? AND trang_thai = 'APPROVED_CMIS'",
                String.class, dtuongQly, month, period
        );
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<String> batchClaimProcessingWorkers(List<String> maKhangs, String month, int period,
                                                     String workerNodeId, int claimTimeoutMinutes) {
        if (maKhangs == null || maKhangs.isEmpty()) return Collections.emptyList();

        // Dùng IN (...) thay vì N lần UPDATE riêng lẻ — giảm N×1000 round-trips xuống còn 1
        StringBuilder inClause = new StringBuilder();
        for (int i = 0; i < maKhangs.size(); i++) {
            if (i > 0) inClause.append(",");
            inClause.append("'").append(maKhangs.get(i).replace("'", "''")).append("'");
        }
        String sql = "UPDATE trang_thai_tinh_toan_kh SET ten_worker = ?, updated_at = NOW() " +
                "WHERE ma_khang IN (" + inClause + ") AND thang_chu_ky = ? AND ky_chot = ? " +
                "AND trang_thai = 'PROCESSING' " +
                "AND (ten_worker IS NULL OR updated_at < NOW() - (? * INTERVAL '1 minute')) " +
                "RETURNING ma_khang";
        try {
            return jdbcTemplate.queryForList(sql, String.class, workerNodeId, month, period, claimTimeoutMinutes);
        } catch (Exception e) {
            log.warn("[BATCH-CLAIM] Batch claim failed, falling back to empty list: {}", e.getMessage());
            return Collections.emptyList();
        }
    }
}
