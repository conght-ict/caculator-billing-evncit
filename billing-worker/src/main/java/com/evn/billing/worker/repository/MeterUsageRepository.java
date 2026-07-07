package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface MeterUsageRepository extends JpaRepository<MeterUsage, MeterUsageId> {
    List<MeterUsage> findByAccountIdAndBillingCycleMonthAndPeriodAndStatus(String accountId, String billingCycleMonth, Integer period, String status);
}
