package com.evn.billing.mediation.repository;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface BillingAccountSnapshotRepository extends JpaRepository<BillingAccountSnapshot, String> {
    Optional<BillingAccountSnapshot> findByMaKhangAndThangChuKyAndKyChotAndPhienBanTinh(
            String maKhang, String thangChuKy, Integer kyChot, Integer phienBanTinh);

    java.util.List<BillingAccountSnapshot> findByMaKhangInAndThangChuKyAndKyChotAndPhienBanTinh(
            java.util.List<String> maKhangList, String thangChuKy, Integer kyChot, Integer phienBanTinh);
}
