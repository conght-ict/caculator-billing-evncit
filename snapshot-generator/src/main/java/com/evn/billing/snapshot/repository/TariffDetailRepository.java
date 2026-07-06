package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.TariffDetail;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface TariffDetailRepository extends JpaRepository<TariffDetail, Long> {
    List<TariffDetail> findByTariffCodeIn(List<String> tariffCodes);
}
