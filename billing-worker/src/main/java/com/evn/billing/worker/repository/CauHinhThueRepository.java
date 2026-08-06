package com.evn.billing.worker.repository;

import com.evn.billing.common.domain.CauHinhThue;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface CauHinhThueRepository extends JpaRepository<CauHinhThue, String> {
    Optional<CauHinhThue> findByLoaiThue(String loaiThue);
}
