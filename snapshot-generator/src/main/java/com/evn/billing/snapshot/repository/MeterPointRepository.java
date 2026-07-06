package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.MeterPoint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface MeterPointRepository extends JpaRepository<MeterPoint, String> {
    List<MeterPoint> findByAccountIdAndStatus(String accountId, String status);
}
