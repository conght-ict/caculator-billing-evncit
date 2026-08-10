package com.evn.billing.worker.repository;

import com.evn.billing.worker.service.BillingLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.sql.Types;

@Repository
public class BillingLogRepositoryImpl implements BillingLogRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void batchInsertCalculationLogs(List<BillingLogService.CalculationLogEntry> entries) {
        String sql = "INSERT INTO nhat_ky_tinh_toan (id_hoa_don, thang_chu_ky, ma_khang, trang_thai, du_lieu_dau_vao, ket_qua_tinh_toan, thong_bao_loi, thoi_gian_xu_ly_ms, ten_worker, created_at) " +
                "VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?)";

        List<Object[]> batchArgs = new ArrayList<>();
        for (BillingLogService.CalculationLogEntry e : entries) {
            batchArgs.add(new Object[] {
                    e.idHoaDon,
                    e.thangChuKy,
                    e.maKhang,
                    e.trangThai,
                    e.duLieuDauVao,
                    e.ketQuaTinhToan,
                    e.thongBaoLoi,
                    e.thoiGianXuLyMs,
                    e.tenWorker,
                    e.createdAt
            });
        }

        int[] argTypes = new int[] {
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.BIGINT,
                Types.VARCHAR,
                Types.TIMESTAMP
        };

        jdbcTemplate.batchUpdate(sql, batchArgs, argTypes);
    }
}
