package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.MeterRelation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface MeterRelationRepository extends JpaRepository<MeterRelation, Long> {
    
    @Query("SELECT r FROM MeterRelation r WHERE (r.maDdoCha IN :meterIds OR r.maDdoCon IN :meterIds) " +
           "AND r.ngayHieuLuc <= CURRENT_DATE AND (r.ngayHetHan IS NULL OR r.ngayHetHan >= CURRENT_DATE)")
    List<MeterRelation> findRelationsByMeterIds(@Param("meterIds") List<String> meterIds);
}
