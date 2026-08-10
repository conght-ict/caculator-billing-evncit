package com.evn.billing.mediation.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.math.BigDecimal;
import java.util.Map;

@Repository
public class CmisSyncRepositoryImpl implements CmisSyncRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void upsertCustomer(String maKhang, String tenKhang, String trangThai, String diaChi, String dienThoai, String email, String maSoThue, String maDviqly) {
        String sql = "INSERT INTO khach_hang (ma_khang, ten_khang, trang_thai, dia_chi, dien_thoai, email, ma_so_thue, ma_dviqly) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                     "ON CONFLICT (ma_khang) DO UPDATE SET " +
                     "ten_khang = EXCLUDED.ten_khang, trang_thai = EXCLUDED.trang_thai, dia_chi = EXCLUDED.dia_chi, " +
                     "dien_thoai = EXCLUDED.dien_thoai, email = EXCLUDED.email, " +
                     "ma_so_thue = EXCLUDED.ma_so_thue, ma_dviqly = EXCLUDED.ma_dviqly";
        jdbcTemplate.update(sql, maKhang, tenKhang, trangThai, diaChi, dienThoai, email, maSoThue, maDviqly);
    }

    @Override
    @Transactional
    public void upsertMeterPoint(String maDdo, String maKhang, String dtuongQly, String maCapda, String trangThai, int loaiDdo, int loaiKhang, boolean isDienMt, String maDviqly) {
        String sql = "INSERT INTO diem_do (ma_ddo, ma_khang, dtuong_qly, ma_capda, trang_thai, loai_ddo, loai_khang, is_dien_mt, ma_dviqly, thong_tin_cto, danh_sach_ap_gia) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '[]'::jsonb, '[]'::jsonb) " +
                     "ON CONFLICT (ma_ddo) DO UPDATE SET " +
                     "ma_khang = EXCLUDED.ma_khang, dtuong_qly = EXCLUDED.dtuong_qly, " +
                     "ma_capda = EXCLUDED.ma_capda, trang_thai = EXCLUDED.trang_thai, " +
                     "loai_ddo = EXCLUDED.loai_ddo, loai_khang = EXCLUDED.loai_khang, " +
                     "is_dien_mt = EXCLUDED.is_dien_mt, ma_dviqly = EXCLUDED.ma_dviqly";
        jdbcTemplate.update(sql, maDdo, maKhang, dtuongQly, maCapda, trangThai, loaiDdo, loaiKhang, isDienMt, maDviqly);
    }

    @Override
    @Transactional
    public void updatePriceRules(String maDdo, String maKhang, String newRulesJson) {
        // Đảm bảo điểm đo tồn tại trong DB trước khi cập nhật áp giá
        try {
            jdbcTemplate.queryForObject("SELECT ma_ddo FROM diem_do WHERE ma_ddo = ?", String.class, maDdo);
        } catch (Exception e) {
            throw new IllegalStateException("Meter point " + maDdo + " not found. Cannot apply price rules to a non-existent meter point.");
        }
        jdbcTemplate.update("UPDATE diem_do SET danh_sach_ap_gia = ?::jsonb WHERE ma_ddo = ?", newRulesJson, maDdo);
    }

    @Override
    @Transactional
    public void upsertMeterRelation(String maDdoCha, String maDdoCon, String loaiQuanHe, String ngayHieuLuc, String ngayHetHan) {
        String checkSql = "SELECT COUNT(*) FROM quan_he_diem_do WHERE ma_ddo_cha = ? AND ma_ddo_con = ?";
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, maDdoCha, maDdoCon);

        if (count != null && count > 0) {
            String sql = "UPDATE quan_he_diem_do SET loai_quan_he = ?, ngay_hieu_luc = CAST(? AS DATE), ngay_het_han = CAST(? AS DATE) " +
                         "WHERE ma_ddo_cha = ? AND ma_ddo_con = ?";
            jdbcTemplate.update(sql, loaiQuanHe, ngayHieuLuc, ngayHetHan, maDdoCha, maDdoCon);
        } else {
            String sql = "INSERT INTO quan_he_diem_do (ma_ddo_cha, ma_ddo_con, loai_quan_he, ngay_hieu_luc, ngay_het_han) " +
                         "VALUES (?, ?, ?, CAST(? AS DATE), CAST(? AS DATE))";
            jdbcTemplate.update(sql, maDdoCha, maDdoCon, loaiQuanHe, ngayHieuLuc, ngayHetHan);
        }
    }

    @Override
    @Transactional
    public void deleteMeterRelation(String maDdoCha, String maDdoCon) {
        String sql = "DELETE FROM quan_he_diem_do WHERE ma_ddo_cha = ? AND ma_ddo_con = ?";
        jdbcTemplate.update(sql, maDdoCha, maDdoCon);
    }

    @Override
    @Transactional
    public void upsertTariff(String maBieuGia, String tenBieuGia, String loaiBieuGia, String ngayHieuLuc, String ngayHetHan, String quyetDinhPhapLy, String trangThai, String chiTietGiaJson,
                             String maNhomnn, String khoangDa, String maNgiaCmis, String thoigianBdien, boolean bacThang, BigDecimal donGiaPhang) {
        String sql = "INSERT INTO bieu_gia (ma_bieu_gia, ten_bieu_gia, loai_bieu_gia, ngay_hieu_luc, ngay_het_han, quyet_dinh_phap_ly, trang_thai, chi_tiet_gia, " +
                     "ma_nhomnn, khoang_da, ma_ngia_cmis, thoigian_bdien, bac_thang, don_gia_phang) " +
                     "VALUES (?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?) " +
                     "ON CONFLICT (ma_bieu_gia) DO UPDATE SET " +
                     "ten_bieu_gia = EXCLUDED.ten_bieu_gia, loai_bieu_gia = EXCLUDED.loai_bieu_gia, " +
                     "ngay_hieu_luc = EXCLUDED.ngay_hieu_luc, ngay_het_han = EXCLUDED.ngay_het_han, " +
                     "quyet_dinh_phap_ly = EXCLUDED.quyet_dinh_phap_ly, trang_thai = EXCLUDED.trang_thai, " +
                     "chi_tiet_gia = EXCLUDED.chi_tiet_gia, ma_nhomnn = EXCLUDED.ma_nhomnn, " +
                     "khoang_da = EXCLUDED.khoang_da, ma_ngia_cmis = EXCLUDED.ma_ngia_cmis, " +
                     "thoigian_bdien = EXCLUDED.thoigian_bdien, bac_thang = EXCLUDED.bac_thang, " +
                     "don_gia_phang = EXCLUDED.don_gia_phang";

        jdbcTemplate.update(sql, maBieuGia, tenBieuGia, loaiBieuGia, ngayHieuLuc, ngayHetHan, quyetDinhPhapLy, trangThai, chiTietGiaJson,
                            maNhomnn, khoangDa, maNgiaCmis, thoigianBdien, bacThang, donGiaPhang);
    }

    @Override
    @Transactional(readOnly = true)
    public List<String> findAccountsByTariff(String maBieuGia) {
        String querySql = "SELECT DISTINCT ma_khang FROM diem_do " +
                          "WHERE danh_sach_ap_gia @> CAST(? AS JSONB)";
        String jsonSearch = "[{\"maNgia\":\"" + maBieuGia + "\"}]";
        return jdbcTemplate.queryForList(querySql, String.class, jsonSearch);
    }

    @Override
    @Transactional(readOnly = true)
    public String findDtuongQlyByKhang(String maKhang) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT dtuong_qly FROM diem_do WHERE ma_khang = ? LIMIT 1", String.class, maKhang);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    @Transactional(readOnly = true)
    public String findCurrentCto(String maDdo) {
        try {
            return jdbcTemplate.queryForObject("SELECT CAST(thong_tin_cto AS TEXT) FROM diem_do WHERE ma_ddo = ?", String.class, maDdo);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    @Transactional
    public void createPlaceholderMeterPoint(String maDdo, String maKhang) {
        throw new UnsupportedOperationException("Creating placeholder meter points is not supported. All meter points must be ingested via cmis-diem-do topic first.");
    }

    @Override
    @Transactional
    public void updateThongTinCto(String maDdo, String thongTinCtoStr) {
        jdbcTemplate.update("UPDATE diem_do SET thong_tin_cto = ?::jsonb WHERE ma_ddo = ?", thongTinCtoStr, maDdo);
    }

    @Override
    @Transactional
    public void upsertMeterPointSchedule(String maDdo, String month, int period, String fromDate, String toDate, String status) {
        String upsertDdoSql = "INSERT INTO lich_ghi_ddo " +
            "(ma_ddo, thang_ck, ky_chot, tu_ngay, den_ngay, tthai_lich, updated_at) " +
            "VALUES (?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, NOW()) " +
            "ON CONFLICT (ma_ddo, thang_ck, ky_chot) DO UPDATE SET " +
            "tu_ngay = EXCLUDED.tu_ngay, den_ngay = EXCLUDED.den_ngay, tthai_lich = EXCLUDED.tthai_lich, " +
            "updated_at = NOW()";
        jdbcTemplate.update(upsertDdoSql, maDdo, month, period, fromDate, toDate, status);
    }

    @Override
    @Transactional
    public void upsertDtuongQlySchedule(String dtuongQly, String month, int period, String fromDate, String toDate, int nMinus, int nPlus, int totalAccounts, String maDviqly) {
        String upsertDqlySql = "INSERT INTO lich_ghi_dqly " +
            "(dtuong_qly, thang_ck, ky_chot, tu_ngay, den_ngay, n_tru, n_cong, tthai_lich, tthai_chay, tong_kh, kh_da_xl, kh_tc, kh_tb, ma_dviqly, snapshot_generated) " +
            "VALUES (?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, 'PENDING', ?, 0, 0, 0, ?, false) " +
            "ON CONFLICT (dtuong_qly, thang_ck, ky_chot) DO UPDATE SET " +
            "tu_ngay = EXCLUDED.tu_ngay, den_ngay = EXCLUDED.den_ngay, n_tru = EXCLUDED.n_tru, n_cong = EXCLUDED.n_cong, " +
            "tthai_lich = EXCLUDED.tthai_lich, tong_kh = EXCLUDED.tong_kh, ma_dviqly = EXCLUDED.ma_dviqly, " +
            "snapshot_generated = CASE WHEN lich_ghi_dqly.den_ngay <> EXCLUDED.den_ngay THEN false ELSE lich_ghi_dqly.snapshot_generated END, " +
            "updated_at = NOW()";
        jdbcTemplate.update(upsertDqlySql, dtuongQly, month, period, fromDate, toDate, nMinus, nPlus, "ACTIVE", totalAccounts, maDviqly);
    }

    @Override
    @Transactional(readOnly = true)
    public Map<String, Object> findActiveBookSchedule(String dtuongQly) {
        try {
            return jdbcTemplate.queryForMap(
                     "SELECT thang_ck, ky_chot FROM lich_ghi_dqly WHERE dtuong_qly = ? AND tthai_lich = 'ACTIVE' ORDER BY updated_at DESC LIMIT 1",
                     dtuongQly);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<Map<String, Object>> findBooksWithUpcomingMeterReadings(int daysAhead) {
        String sql = "SELECT dtuong_qly, thang_ck, ky_chot, den_ngay FROM lich_ghi_dqly " +
                     "WHERE den_ngay >= CURRENT_DATE AND den_ngay <= CURRENT_DATE + ? " +
                     "AND snapshot_generated = false AND tthai_lich = 'ACTIVE'";
        return jdbcTemplate.queryForList(sql, daysAhead);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Map<String, Object>> findActiveBookSchedules(String dtuongQly) {
        try {
            return jdbcTemplate.queryForList(
                "SELECT thang_ck, ky_chot FROM lich_ghi_dqly " +
                "WHERE dtuong_qly = ? AND tthai_lich IN ('ACTIVE','PROCESSING') " +
                "ORDER BY den_ngay DESC",
                dtuongQly);
        } catch (Exception e) {
            return new java.util.ArrayList<>();
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<Map<String, Object>> findBooksByTariff(String maBieuGia) {
        try {
            return jdbcTemplate.queryForList(
                "SELECT DISTINCT dd.dtuong_qly, lg.thang_ck, lg.ky_chot " +
                "FROM diem_do dd " +
                "JOIN lich_ghi_dqly lg ON lg.dtuong_qly = dd.dtuong_qly AND lg.tthai_lich = 'ACTIVE' " +
                "WHERE dd.danh_sach_ap_gia::text LIKE ? AND dd.trang_thai = 'ACTIVE'",
                "%" + maBieuGia + "%"
            );
        } catch (Exception e) {
            return new java.util.ArrayList<>();
        }
    }
}

