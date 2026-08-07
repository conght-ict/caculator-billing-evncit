package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface BillingAccountSnapshotRepository extends JpaRepository<BillingAccountSnapshot, String> {
    Optional<BillingAccountSnapshot> findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(
            String maKhang, String thangChuKy, Integer kyChot, Integer phienBanTinh);

    @Query(value = "SELECT CAST(du_lieu_cau_hinh AS TEXT) FROM snapshot_tinh_toan " +
        "WHERE ma_khang = :maKhang AND thang_chu_ky = :month AND ky_chot = :period AND phien_ban_tinh = :version",
        nativeQuery = true)
    String findSnapshotJson(
        @Param("maKhang") String maKhang,
        @Param("month") String month,
        @Param("period") Integer period,
        @Param("version") Integer version);
}
