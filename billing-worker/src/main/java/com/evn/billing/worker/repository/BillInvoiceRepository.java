package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillInvoiceId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface BillInvoiceRepository extends JpaRepository<BillInvoice, BillInvoiceId> {
    Optional<BillInvoice> findByAccountIdAndBillingCycleMonthAndPeriod(String accountId, String billingCycleMonth, Integer period);
}
