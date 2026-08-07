package com.evn.billing.worker.repository;

import com.evn.billing.worker.service.BillingLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class BillingLogRepositoryImpl implements BillingLogRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void batchInsertCalculationLogs(List<BillingLogService.CalculationLogEntry> entries) {
        String sql = "INSERT INTO nhat_ky_tinh_toan (id_log, dtuong_qly, ma_khang, thang_chu_ky, ky_chot, trang_thai, du_lieu_vao, du_lieu_ra, thong_bao_loi, created_at) " +
                "VALUES (?::uuid, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)";

        List<Object[]> batchArgs = new ArrayList<>();
        for (BillingLogService.CalculationLogEntry e : entries) {
            batchArgs.add(new Object[] {
                    e.logId.toString(),
                    e.dtuongQly,
                    e.maKhang,
                    e.billingCycleMonth,
                    e.period,
                    e.status,
                    e.inputData,
                    e.outputData,
                    e.errorMessage,
                    e.createdAt
            });
        }

        int[] argTypes = new int[] {
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.INTEGER,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.TIMESTAMP
        };

        jdbcTemplate.batchUpdate(sql, batchArgs, argTypes);
    }
}
