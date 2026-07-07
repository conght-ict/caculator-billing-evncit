package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.AccountBillingStatus;
import com.evn.billing.common.domain.AccountBillingStatusId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface AccountBillingStatusRepository extends JpaRepository<AccountBillingStatus, AccountBillingStatusId> {
    List<AccountBillingStatus> findByBookIdAndBillingCycleMonthAndPeriod(String bookId, String billingCycleMonth, Integer period);
}
