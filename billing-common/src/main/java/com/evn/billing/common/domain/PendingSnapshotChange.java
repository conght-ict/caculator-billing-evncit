package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "pending_snapshot_change")
@Data
public class PendingSnapshotChange {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_thay_doi")
    private Long idThayDoi;

    @Column(name = "ma_khang", length = 50, nullable = false)
    private String maKhang;

    @Column(name = "thang_chu_ky", length = 20, nullable = false)
    private String thangChuKy;

    @Column(name = "ky_chot", nullable = false)
    private Integer kyChot = 1;

    @Column(name = "rule_id", length = 10, nullable = false)
    private String ruleId;

    @Column(name = "bang_nguon", length = 50, nullable = false)
    private String bangNguon;

    @Column(name = "truong_thay_doi", nullable = false)
    private String truongThayDoi;

    @Column(name = "du_lieu_cu")
    private String duLieuCu; // JSON String

    @Column(name = "du_lieu_moi")
    private String duLieuMoi; // JSON String

    @Column(name = "trang_thai", length = 20, nullable = false)
    private String trangThai = "PENDING"; // PENDING | PROCESSED | SKIPPED

    @Column(name = "grace_expires_at", nullable = false)
    private LocalDateTime graceExpiresAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "processed_at")
    private LocalDateTime processedAt;
}
