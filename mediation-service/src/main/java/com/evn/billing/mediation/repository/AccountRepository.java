package com.evn.billing.mediation.repository;

import com.evn.billing.common.domain.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface AccountRepository extends JpaRepository<Account, String> {
    @org.springframework.data.jpa.repository.Query("SELECT DISTINCT a FROM Account a JOIN MeterPoint mp ON mp.maKhang = a.maKhang WHERE mp.dtuongQly = :dtuongQly")
    List<Account> findByDtuongQly(@org.springframework.data.repository.query.Param("dtuongQly") String dtuongQly);
    List<Account> findTop100ByOrderByMaKhangAsc();
}
