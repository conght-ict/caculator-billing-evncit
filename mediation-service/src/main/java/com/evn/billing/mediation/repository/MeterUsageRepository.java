package com.evn.billing.mediation.repository;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface MeterUsageRepository extends JpaRepository<MeterUsage, MeterUsageId> {

    List<MeterUsage> findByMaKhangAndThangChuKyAndKyChot(String maKhang, String thangChuKy, Integer kyChot);

    List<MeterUsage> findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(String maKhang, String thangChuKy, Integer kyChot, String trangThaiXuLy);

    @Query("SELECT m FROM MeterUsage m WHERE m.thangChuKy = :month AND m.kyChot = :period AND m.trangThaiXuLy = 'PENDING_MANUAL'")
    List<MeterUsage> findPendingManualByMonthAndPeriod(@Param("month") String month, @Param("period") Integer period);

    Optional<MeterUsage> findByMaKhangAndMaDdoAndThangChuKyAndKyChot(String maKhang, String maDdo, String thangChuKy, Integer kyChot);

    @Query("SELECT COUNT(m) FROM MeterUsage m WHERE m.maKhang IN " +
           "(SELECT mp.maKhang FROM MeterPoint mp WHERE mp.dtuongQly = :dtuongQly AND mp.trangThai = 'ACTIVE') " +
           "AND m.thangChuKy = :month AND m.kyChot = :period AND m.trangThaiXuLy = 'PENDING_MANUAL'")
    long countPendingReadingsForBook(@Param("dtuongQly") String dtuongQly, @Param("month") String month, @Param("period") Integer period);
}
