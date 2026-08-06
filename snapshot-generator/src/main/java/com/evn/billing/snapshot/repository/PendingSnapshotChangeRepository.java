package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.PendingSnapshotChange;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface PendingSnapshotChangeRepository extends JpaRepository<PendingSnapshotChange, Long> {
    List<PendingSnapshotChange> findByTrangThaiAndGraceExpiresAtLessThanEqual(String trangThai, LocalDateTime time);
}
