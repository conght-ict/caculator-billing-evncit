package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.MeterRelation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface MeterRelationRepository extends JpaRepository<MeterRelation, Long> {
    
    @Query("SELECT r FROM MeterRelation r WHERE r.parentId IN :meterIds OR r.childId IN :meterIds")
    List<MeterRelation> findRelationsByMeterIds(@Param("meterIds") List<String> meterIds);
}
