package com.evn.billing.snapshot.job;

import com.evn.billing.common.domain.BillingAccountSnapshot;
import com.evn.billing.common.domain.PendingSnapshotChange;
import com.evn.billing.snapshot.repository.BillingAccountSnapshotRepository;
import com.evn.billing.snapshot.repository.PendingSnapshotChangeRepository;
import com.evn.billing.snapshot.service.SnapshotGeneratorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Component
@Slf4j
public class SnapshotGracePeriodJob {

    @Autowired
    private PendingSnapshotChangeRepository pendingSnapshotChangeRepository;

    @Autowired
    private BillingAccountSnapshotRepository snapshotRepository;

    @Autowired
    private SnapshotGeneratorService snapshotGeneratorService;

    /**
     * Định kỳ quét các yêu cầu thay đổi dữ liệu khi snapshot bị LOCKED và grace period đã hết hạn.
     * Chạy mỗi 30 phút.
     */
    @Scheduled(cron = "0 */30 * * * *")
    public void runPendingSnapshotChanges() {
        log.info("[GRACE-PERIOD-JOB] Checking expired pending snapshot changes...");
        try {
            List<PendingSnapshotChange> pendingList = pendingSnapshotChangeRepository
                    .findByTrangThaiAndGraceExpiresAtLessThanEqual("PENDING", LocalDateTime.now());

            if (pendingList.isEmpty()) {
                log.info("[GRACE-PERIOD-JOB] No expired pending snapshot changes found.");
                return;
            }

            log.info("[GRACE-PERIOD-JOB] Found {} expired pending changes. Processing...", pendingList.size());
            
            for (PendingSnapshotChange pending : pendingList) {
                String maKhang = pending.getMaKhang();
                String month = pending.getThangChuKy();
                Integer period = pending.getKyChot();

                try {
                    log.info("[GRACE-PERIOD-JOB] Auto-regenerating snapshot for account: {}, month: {}, period: {}, rule: {}", 
                             maKhang, month, period, pending.getRuleId());
                    
                    // Set snapshot status back to DRAFT using snapshotRepository
                    Optional<BillingAccountSnapshot> snapOpt = snapshotRepository.findByMaKhangAndThangChuKyAndKyChot(maKhang, month, period);
                    if (snapOpt.isPresent()) {
                        BillingAccountSnapshot snap = snapOpt.get();
                        snap.setTrangThai("DRAFT");
                        snapshotRepository.save(snap);
                    }

                    // Rebuild snapshot
                    snapshotGeneratorService.generateSnapshotForAccount(maKhang, month, period, pending.getRuleId(), pending.getBangNguon(), pending.getTruongThayDoi());

                    // Mark pending change as PROCESSED
                    pending.setTrangThai("PROCESSED");
                    pending.setProcessedAt(LocalDateTime.now());
                    pendingSnapshotChangeRepository.save(pending);
                    
                    log.info("[GRACE-PERIOD-JOB] Successfully processed change ID: {} for account: {}", pending.getIdThayDoi(), maKhang);
                } catch (Exception e) {
                    log.error("[GRACE-PERIOD-JOB] Failed to process change ID: {} for account: {}. Error: {}", pending.getIdThayDoi(), maKhang, e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("[GRACE-PERIOD-JOB] Error checking expired pending changes", e);
        }
    }
}
