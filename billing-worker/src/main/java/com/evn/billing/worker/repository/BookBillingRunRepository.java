package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.BookBillingRun;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface BookBillingRunRepository extends JpaRepository<BookBillingRun, UUID> {
    Optional<BookBillingRun> findByBookIdAndBillingCycleMonth(String bookId, String billingCycleMonth);
}
