package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.DtuongQlySchedule;
import com.evn.billing.common.domain.DtuongQlyScheduleId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface DtuongQlyScheduleRepository extends JpaRepository<DtuongQlySchedule, DtuongQlyScheduleId> {
    Optional<DtuongQlySchedule> findByDtuongQlyAndThangCkAndKyChot(String dtuongQly, String thangCk, Integer kyChot);
}
