package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

@Repository
public interface AccountRepository extends JpaRepository<Account, String> {
    @Query("SELECT DISTINCT a FROM Account a JOIN MeterPoint mp ON mp.maKhang = a.maKhang WHERE mp.dtuongQly = :dtuongQly AND a.trangThai = :status")
    List<Account> findByDtuongQlyAndStatus(@Param("dtuongQly") String dtuongQly, @Param("status") String status);
}
