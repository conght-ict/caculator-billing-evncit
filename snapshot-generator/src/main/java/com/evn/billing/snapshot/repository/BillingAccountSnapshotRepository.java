package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface BillingAccountSnapshotRepository extends JpaRepository<BillingAccountSnapshot, String> {
    List<BillingAccountSnapshot> findByBookIdAndBillingCycleMonthAndPeriod(String bookId, String billingCycleMonth, Integer period);
}
