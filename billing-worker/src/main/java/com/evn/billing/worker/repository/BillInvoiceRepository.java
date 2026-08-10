package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillInvoiceId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

@Repository
public interface BillInvoiceRepository extends JpaRepository<BillInvoice, BillInvoiceId> {
    Optional<BillInvoice> findByMaKhangAndThangChuKyAndKyChot(String maKhang, String thangChuKy, Integer kyChot);
 
    @Query("SELECT COUNT(b) FROM BillInvoice b WHERE b.maKhang = :maKhang AND b.thangChuKy = :thangChuKy AND b.kyChot = :kyChot")
    long countByMaKhangAndThangChuKyAndKyChot(@Param("maKhang") String maKhang, 
                                                       @Param("thangChuKy") String thangChuKy, 
                                                       @Param("kyChot") Integer kyChot);
}
