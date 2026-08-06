package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.DiemDoSchedule;
import com.evn.billing.common.domain.DiemDoScheduleId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface DiemDoScheduleRepository extends JpaRepository<DiemDoSchedule, DiemDoScheduleId> {
    Optional<DiemDoSchedule> findByMaDdoAndThangCkAndKyChot(String maDdo, String thangCk, Integer kyChot);
}
