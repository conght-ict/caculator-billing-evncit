package com.evn.billing.worker.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@Repository
public class BillingWorkerRepositoryImpl implements BillingWorkerRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void saveBillingResults(
            List<InvoiceInsertParam> invoices,
            List<OutboxInsertParam> outboxEvents,
            List<StatusUpdateParam> statuses) {

        // 1. Bulk insert Invoices (using raw SQL batch for max performance)
        if (invoices != null && !invoices.isEmpty()) {
            String invoiceSql = "INSERT INTO hoa_don (" +
                    "id_hoa_don, ma_khang, dtuong_qly, thang_chu_ky, ky_chot, " +
                    "tong_tien_truoc_thue, tien_thue, tong_tien_sau_thue, " +
                    "khoa_lap_trung, ban_ke_tinh_toan, ref_snapshot, trang_thai_tinh_toan, " +
                    "ma_dviqly, created_at, updated_at" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, 'FINAL', ?, NOW(), NOW()) " +
                    "ON CONFLICT (khoa_lap_trung, thang_chu_ky) DO NOTHING";

            jdbcTemplate.batchUpdate(invoiceSql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    InvoiceInsertParam inv = invoices.get(i);
                    ps.setString(1, inv.getId());
                    ps.setString(2, inv.getCustomerId());
                    ps.setString(3, inv.getDtuongQly());
                    ps.setString(4, inv.getCycleMonth());
                    ps.setInt(5, inv.getPeriod());
                    ps.setBigDecimal(6, inv.getAmountBeforeTax());
                    ps.setBigDecimal(7, inv.getTaxAmount());
                    ps.setBigDecimal(8, inv.getAmountAfterTax());
                    ps.setString(9, inv.getIdempotencyKey());
                    ps.setString(10, inv.getCalculationManifestJson());
                    ps.setString(11, inv.getRefSnapshotId());
                    ps.setString(12, inv.getMaDviqly() != null ? inv.getMaDviqly() : "PD0600");
                }

                @Override
                public int getBatchSize() {
                    return invoices.size();
                }
            });
        }

        // 2. Bulk insert Outbox Events (Deduplicated via unique index)
        if (outboxEvents != null && !outboxEvents.isEmpty()) {
            String outboxSql = "INSERT INTO su_kien_outbox (" +
                    "loai_doi_tuong, id_doi_tuong, loai_su_kien, noi_dung, trang_thai, created_at" +
                    ") VALUES (?, ?, ?, ?::jsonb, 'PENDING', NOW()) " +
                    "ON CONFLICT (loai_doi_tuong, id_doi_tuong, loai_su_kien) DO NOTHING";

            jdbcTemplate.batchUpdate(outboxSql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    OutboxInsertParam event = outboxEvents.get(i);
                    ps.setString(1, event.getAggregateType());
                    ps.setString(2, event.getAggregateId());
                    ps.setString(3, event.getEventType());
                    ps.setString(4, event.getPayloadJson());
                }

                @Override
                public int getBatchSize() {
                    return outboxEvents.size();
                }
            });
        }

        // 3. Bulk update Customers Billing Process status
        if (statuses != null && !statuses.isEmpty()) {
            String statusSql = "INSERT INTO trang_thai_tinh_toan_kh " +
                    "(ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, id_hoa_don, thong_bao_loi, thoi_gian_xu_ly_ms, updated_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW()) " +
                    "ON CONFLICT (ma_khang, thang_chu_ky, ky_chot) DO UPDATE SET " +
                    "trang_thai = EXCLUDED.trang_thai, " +
                    "dtuong_qly = EXCLUDED.dtuong_qly, " +
                    "id_hoa_don = EXCLUDED.id_hoa_don, " +
                    "thong_bao_loi = EXCLUDED.thong_bao_loi, " +
                    "thoi_gian_xu_ly_ms = EXCLUDED.thoi_gian_xu_ly_ms, " +
                    "updated_at = NOW()";

            jdbcTemplate.batchUpdate(statusSql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    StatusUpdateParam st = statuses.get(i);
                    ps.setString(1, st.getCustomerId());
                    ps.setString(2, st.getCycleMonth());
                    ps.setString(3, st.getDtuongQly());
                    ps.setInt(4, st.getPeriod());
                    ps.setString(5, st.getStatus());
                    ps.setString(6, st.getInvoiceId());
                    ps.setString(7, st.getErrorMessage());
                    ps.setLong(8, st.getProcessingTimeMs());
                }

                @Override
                public int getBatchSize() {
                    return statuses.size();
                }
            });
        }
    }
}
