package com.evn.billing.batch.repository;

import com.evn.billing.common.domain.MeterUsage;
import com.evn.billing.common.domain.MeterUsageId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface MeterUsageRepository extends JpaRepository<MeterUsage, MeterUsageId> {

    /**
     * Counts the number of meter points in a specific Book that are missing validated readings
     * or are in PENDING_MANUAL status.
     */
    @Query("SELECT COUNT(m) FROM MeterUsage m WHERE m.accountId IN " +
           "(SELECT a.accountId FROM Account a WHERE a.bookId = :bookId AND a.status = 'ACTIVE') " +
           "AND m.billingCycleMonth = :month AND m.status = 'PENDING_MANUAL'")
    long countPendingReadingsForBook(@Param("bookId") String bookId, @Param("month") String month);

    java.util.List<MeterUsage> findByAccountIdAndBillingCycleMonthAndStatus(String accountId, String billingCycleMonth, String status);
}
