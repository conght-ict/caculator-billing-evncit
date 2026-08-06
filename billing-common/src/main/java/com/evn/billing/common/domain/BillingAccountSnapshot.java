package com.evn.billing.common.domain;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "snapshot_tinh_toan")
@Getter
@Setter
public class BillingAccountSnapshot {

    @Id
    @Column(name = "id_snapshot", length = 200)
    private String idSnapshot; // {account_id}_{billing_cycle_month}_v{version}

    @Column(name = "ma_khang", length = 50, nullable = false)
    private String maKhang;

    @Column(name = "dtuong_qly", length = 50, nullable = false)
    private String dtuongQly;

    @Column(name = "thang_chu_ky", length = 20, nullable = false)
    private String thangChuKy;

    @Column(name = "ky_chot", nullable = false)
    private Integer kyChot = 1;

    @Column(name = "phien_ban_tinh", nullable = false)
    private Integer phienBanTinh = 1;

    @Column(name = "trang_thai", length = 20, nullable = false)
    private String trangThai = "DRAFT"; // DRAFT | LOCKED | DEPRECATED

    @Column(name = "ngay_dong_bo_hieu_luc", nullable = false)
    private LocalDate ngayDongBoHieuLuc;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "du_lieu_cau_hinh", nullable = false)
    private BillingConfigSnapshot duLieuCauHinh;

    @Column(name = "ma_dviqly", length = 20, nullable = false)
    private String maDviqly = "PD0600";

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
}
