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

    List<MeterUsage> findByAccountIdAndBillingCycleMonth(String accountId, String billingCycleMonth);

    List<MeterUsage> findByAccountIdAndBillingCycleMonthAndStatus(String accountId, String billingCycleMonth, String status);

    @Query("SELECT m FROM MeterUsage m WHERE m.billingCycleMonth = :month AND m.status = 'PENDING_MANUAL'")
    List<MeterUsage> findPendingManualByMonth(@Param("month") String month);

    Optional<MeterUsage> findByAccountIdAndMeterPointIdAndBillingCycleMonth(String accountId, String meterPointId, String billingCycleMonth);
}
