package com.evn.billing.mediation.repository;

import java.util.List;

public interface CmisSyncRepository {
    void upsertCustomer(String maKhang, String tenKhang, String trangThai, String diaChi, String dienThoai, String email, String maSoThue, String maDviqly);
    void upsertMeterPoint(String maDdo, String maKhang, String dtuongQly, String maCapda, String trangThai, int loaiDdo, int loaiKhang, boolean isDienMt, String maDviqly);
    void updatePriceRules(String maDdo, String maKhang, String newRulesJson);
    void upsertMeterRelation(String maDdoCha, String maDdoCon, String loaiQuanHe, String ngayHieuLuc, String ngayHetHan);
    void deleteMeterRelation(String maDdoCha, String maDdoCon);
    void upsertTariff(String maBieuGia, String tenBieuGia, String loaiBieuGia, String ngayHieuLuc, String ngayHetHan, String quyetDinhPhapLy, String trangThai, String chiTietGiaJson);
    List<String> findAccountsByTariff(String maBieuGia);
    String findDtuongQlyByKhang(String maKhang);
    String findCurrentCto(String maDdo);
    void createPlaceholderMeterPoint(String maDdo, String maKhang);
    void updateThongTinCto(String maDdo, String thongTinCtoStr);
    void upsertMeterPointSchedule(String maDdo, String month, int period, String fromDate, String toDate, String status);
    void upsertDtuongQlySchedule(String dtuongQly, String month, int period, String fromDate, String toDate, int nMinus, int nPlus, int totalAccounts, String maDviqly);
    java.util.Map<String, Object> findActiveBookSchedule(String dtuongQly);
}
