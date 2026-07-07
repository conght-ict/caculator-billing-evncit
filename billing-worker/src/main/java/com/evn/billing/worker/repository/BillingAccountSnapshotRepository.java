package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface BillingAccountSnapshotRepository extends JpaRepository<BillingAccountSnapshot, String> {
    Optional<BillingAccountSnapshot> findByAccountIdAndBillingCycleMonthAndPeriodAndCalculationVersion(
            String accountId, String billingCycleMonth, Integer period, Integer calculationVersion);
}
