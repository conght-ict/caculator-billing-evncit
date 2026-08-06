package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.AccountBillingStatus;
import com.evn.billing.common.domain.AccountBillingStatusId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface AccountBillingStatusRepository extends JpaRepository<AccountBillingStatus, AccountBillingStatusId> {
    List<AccountBillingStatus> findByDtuongQlyAndThangChuKyAndKyChot(String dtuongQly, String thangChuKy, Integer kyChot);
    Page<AccountBillingStatus> findByDtuongQlyAndThangChuKyAndKyChotAndTrangThaiIn(String dtuongQly, String thangChuKy, Integer kyChot, List<String> trangThaiList, Pageable pageable);

    @Query(value = "SELECT * FROM trang_thai_tinh_toan_kh WHERE ma_khang = :maKhang AND thang_chu_ky = :thangChuKy AND ky_chot = :kyChot FOR UPDATE", nativeQuery = true)
    Optional<AccountBillingStatus> findByIdForUpdate(@Param("maKhang") String maKhang,
                                                     @Param("thangChuKy") String thangChuKy,
                                                     @Param("kyChot") Integer kyChot);
}
