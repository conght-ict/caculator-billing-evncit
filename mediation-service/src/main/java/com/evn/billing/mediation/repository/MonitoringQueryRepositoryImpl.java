package com.evn.billing.mediation.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class MonitoringQueryRepositoryImpl implements MonitoringQueryRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void initPendingStatuses(String month, int period, String dtuongQly) {
        String initSql = "INSERT INTO trang_thai_tinh_toan_kh (ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, updated_at) " +
                "SELECT DISTINCT d.ma_khang, ?, d.dtuong_qly, ?, 'PENDING', NOW() " +
                "FROM diem_do d " +
                "JOIN khach_hang k ON d.ma_khang = k.ma_khang " +
                "WHERE d.dtuong_qly = ? " +
                "ON CONFLICT (ma_khang, thang_chu_ky, ky_chot) DO NOTHING";
        jdbcTemplate.update(initSql, month, period, dtuongQly);
    }

    @Override
    public List<Map<String, Object>> findAccountsWithStatus(String month, int period, String dtuongQly) {
        String selectSql = "SELECT DISTINCT k.ma_khang AS \"accountId\", k.ten_khang AS \"customerName\", " +
                "COALESCE(t.trang_thai, 'PENDING') AS \"status\", " +
                "t.thong_bao_loi AS \"errorMessage\", t.id_hoa_don AS \"invoiceId\", t.updated_at AS \"updatedAt\" " +
                "FROM khach_hang k " +
                "JOIN diem_do d ON d.ma_khang = k.ma_khang " +
                "LEFT JOIN trang_thai_tinh_toan_kh t ON t.ma_khang = k.ma_khang AND t.thang_chu_ky = ? AND t.ky_chot = ? " +
                "WHERE d.dtuong_qly = ? " +
                "ORDER BY k.ma_khang";
        return jdbcTemplate.queryForList(selectSql, month, period, dtuongQly);
    }

    @Override
    public List<Map<String, Object>> findCalculationLogs(String dtuongQly, String accountId, String status, int limit) {
        StringBuilder query = new StringBuilder(
                "SELECT id_log AS log_id, dtuong_qly AS dtuong_qly, ma_khang AS account_id, thang_chu_ky AS billing_cycle_month, trang_thai AS status, thong_bao_loi AS error_message, created_at FROM nhat_ky_tinh_toan WHERE 1=1 ");
        List<Object> args = new ArrayList<>();

        if (dtuongQly != null && !dtuongQly.trim().isEmpty()) {
            query.append("AND dtuong_qly = ? ");
            args.add(dtuongQly.trim());
        }
        if (accountId != null && !accountId.trim().isEmpty()) {
            query.append("AND ma_khang = ? ");
            args.add(accountId.trim());
        }
        if (status != null && !status.trim().isEmpty()) {
            query.append("AND trang_thai = ? ");
            args.add(status.trim());
        }

        query.append("ORDER BY created_at DESC LIMIT ?");
        args.add(limit);

        return jdbcTemplate.queryForList(query.toString(), args.toArray());
    }

    @Override
    public Optional<Map<String, Object>> findCalculationLogDetail(String logId) {
        try {
            Map<String, Object> logEntry = jdbcTemplate.queryForMap(
                    "SELECT id_log AS log_id, dtuong_qly AS dtuong_qly, ma_khang AS account_id, thang_chu_ky AS billing_cycle_month, trang_thai AS status, du_lieu_vao AS input_data, du_lieu_ra AS output_data, thong_bao_loi AS error_message, created_at FROM nhat_ky_tinh_toan WHERE id_log = ?::uuid",
                    logId);
            return Optional.of(logEntry);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public List<Map<String, Object>> findErrorLogs(String accountId, String month, Integer period, int limit) {
        StringBuilder query = new StringBuilder(
                "SELECT id_loi AS error_id, ma_khang AS account_id, thang_chu_ky AS billing_cycle_month, ky_chot AS period, tin_nhan_loi AS error_message, stack_trace, created_at " +
                "FROM nhat_ky_loi_tinh_toan WHERE 1=1 ");
        List<Object> args = new ArrayList<>();

        if (accountId != null && !accountId.trim().isEmpty()) {
            query.append("AND ma_khang = ? ");
            args.add(accountId.trim());
        }
        if (month != null && !month.trim().isEmpty()) {
            query.append("AND thang_chu_ky = ? ");
            args.add(month.trim());
        }
        if (period != null) {
            query.append("AND ky_chot = ? ");
            args.add(period);
        }

        query.append("ORDER BY created_at DESC LIMIT ?");
        args.add(limit);

        return jdbcTemplate.queryForList(query.toString(), args.toArray());
    }

    @Override
    public List<Map<String, Object>> findBatchExecutions() {
        String sql = "SELECT e.job_execution_id, i.job_name, e.status, e.start_time, e.end_time, e.exit_code, e.exit_message, " +
                "(SELECT parameter_value FROM batch_job_execution_params WHERE job_execution_id = e.job_execution_id AND parameter_name = 'dtuongQly') as book_id, " +
                "(SELECT parameter_value FROM batch_job_execution_params WHERE job_execution_id = e.job_execution_id AND parameter_name = 'month') as month " +
                "FROM batch_job_execution e " +
                "JOIN batch_job_instance i ON e.job_instance_id = i.job_instance_id " +
                "ORDER BY e.job_execution_id DESC LIMIT 50";
        return jdbcTemplate.queryForList(sql);
    }

    @Override
    public List<Map<String, Object>> findBookBillingRuns() {
        String sql = "SELECT dtuong_qly AS book_id, thang_ck AS billing_cycle_month, ky_chot AS period, tthai_lich AS status, tthai_chay AS run_status, tong_kh AS total_accounts, kh_da_xl AS processed_accounts, kh_tc AS success_accounts, kh_tb AS failed_accounts, updated_at " +
                "FROM lich_ghi_dqly ORDER BY updated_at DESC LIMIT 50";
        return jdbcTemplate.queryForList(sql);
    }

    @Override
    public List<Map<String, Object>> findBatchStepExecutions(Long jobExecutionId) {
        String sql = "SELECT step_name, status, start_time, end_time, read_count, write_count, exit_code, " +
                "EXTRACT(EPOCH FROM (COALESCE(end_time, NOW()) - start_time)) as duration_seconds " +
                "FROM batch_step_execution " +
                "WHERE job_execution_id = ? " +
                "ORDER BY step_execution_id ASC";
        return jdbcTemplate.queryForList(sql, jobExecutionId);
    }

    @Override
    public List<Long> findLatestJobExecutionIds(String dtuongQly, String month) {
        String findJobIdSql = "SELECT e.job_execution_id FROM batch_job_execution e " +
                "JOIN batch_job_execution_params p1 ON e.job_execution_id = p1.job_execution_id AND p1.parameter_name = 'dtuongQly' AND p1.parameter_value = ? " +
                "JOIN batch_job_execution_params p2 ON e.job_execution_id = p2.job_execution_id AND p2.parameter_name = 'month' AND p2.parameter_value = ? " +
                "ORDER BY e.job_execution_id DESC LIMIT 1";
        return jdbcTemplate.queryForList(findJobIdSql, Long.class, dtuongQly, month);
    }

    @Override
    public Optional<Map<String, Object>> findLatestActiveScheduleByBook(String dtuongQly) {
        try {
            String sql = "SELECT thang_ck, ky_chot FROM lich_ghi_dqly " +
                    "WHERE dtuong_qly = ? AND tthai_lich = 'ACTIVE' ORDER BY updated_at DESC LIMIT 1";
            return Optional.of(jdbcTemplate.queryForMap(sql, dtuongQly));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public String findBookByAccountId(String accountId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT dtuong_qly FROM diem_do WHERE ma_khang = ? LIMIT 1", String.class, accountId);
        } catch (Exception e) {
            return null;
        }
    }
}
