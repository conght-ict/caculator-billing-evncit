package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillInvoiceId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface BillInvoiceRepository extends JpaRepository<BillInvoice, BillInvoiceId> {
    Optional<BillInvoice> findByMaKhangAndThangChuKyAndKyChot(String maKhang, String thangChuKy, Integer kyChot);
 
    @org.springframework.data.jpa.repository.Query("SELECT COUNT(b) FROM BillInvoice b WHERE b.maKhang = :maKhang AND b.thangChuKy = :thangChuKy AND b.kyChot = :kyChot")
    long countByMaKhangAndThangChuKyAndKyChot(@org.springframework.data.repository.query.Param("maKhang") String maKhang, 
                                                       @org.springframework.data.repository.query.Param("thangChuKy") String thangChuKy, 
                                                       @org.springframework.data.repository.query.Param("kyChot") Integer kyChot);
}
