package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface AccountRepository extends JpaRepository<Account, String> {
    List<Account> findByBookIdAndStatus(String bookId, String status);
}
