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

    List<MeterUsage> findByAccountIdAndBillingCycleMonthAndPeriod(String accountId, String billingCycleMonth, Integer period);

    List<MeterUsage> findByAccountIdAndBillingCycleMonthAndPeriodAndStatus(String accountId, String billingCycleMonth, Integer period, String status);

    @Query("SELECT m FROM MeterUsage m WHERE m.billingCycleMonth = :month AND m.period = :period AND m.status = 'PENDING_MANUAL'")
    List<MeterUsage> findPendingManualByMonthAndPeriod(@Param("month") String month, @Param("period") Integer period);

    Optional<MeterUsage> findByAccountIdAndMeterPointIdAndBillingCycleMonthAndPeriod(String accountId, String meterPointId, String billingCycleMonth, Integer period);
}
