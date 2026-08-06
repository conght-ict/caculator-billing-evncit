package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

import java.util.Optional;

@Repository
public interface BillingAccountSnapshotRepository extends JpaRepository<BillingAccountSnapshot, String> {
    List<BillingAccountSnapshot> findByDtuongQlyAndThangChuKyAndKyChot(String dtuongQly, String thangChuKy, Integer kyChot);
    Optional<BillingAccountSnapshot> findByMaKhangAndThangChuKyAndKyChot(String maKhang, String thangChuKy, Integer kyChot);
}
