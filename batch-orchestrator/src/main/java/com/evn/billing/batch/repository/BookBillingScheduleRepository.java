package com.evn.billing.batch.repository;

import com.evn.billing.common.domain.BookBillingSchedule;
import com.evn.billing.common.domain.BookBillingScheduleId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface BookBillingScheduleRepository extends JpaRepository<BookBillingSchedule, BookBillingScheduleId> {
    Optional<BookBillingSchedule> findByBookIdAndBillingCycleMonthAndPeriod(String bookId, String billingCycleMonth, Integer period);
}
