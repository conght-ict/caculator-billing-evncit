package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.PendingSnapshotChange;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;

@Repository
public interface PendingSnapshotChangeRepository extends JpaRepository<PendingSnapshotChange, Long> {
    Page<PendingSnapshotChange> findByTrangThaiAndGraceExpiresAtLessThanEqual(String trangThai, LocalDateTime time, Pageable pageable);
}
