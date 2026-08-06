package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "chi_so_dien_nang")
@IdClass(MeterUsageId.class)
@Getter
@Setter
public class MeterUsage {

    @Id
    @Column(name = "id_chi_so")
    private Long idChiSo;

    @Id
    @Column(name = "lan_doc_phu")
    private Integer lanDocPhu = 1;

    @Id
    @Column(name = "thang_chu_ky", length = 20)
    private String thangChuKy; // Format: YYYY_MM

    @Id
    @Column(name = "ky_chot")
    private Integer kyChot = 1;

    @Column(name = "ma_khang", length = 50, nullable = false)
    private String maKhang;

    @Column(name = "ma_ddo", length = 50, nullable = false)
    private String maDdo;

    @Column(name = "tu_ngay", nullable = false)
    private LocalDateTime tuNgay;

    @Column(name = "den_ngay", nullable = false)
    private LocalDateTime denNgay;

    @Column(name = "chi_so_dau", nullable = false, precision = 14, scale = 2)
    private BigDecimal chiSoDau;

    @Column(name = "chi_so_cuoi", nullable = false, precision = 14, scale = 2)
    private BigDecimal chiSoCuoi;

    @Column(name = "co_quay_vong", nullable = false)
    private Boolean coQuayVong = false;

    @Transient
    private BigDecimal maxRegisterSnapshot; // Managed in-memory, no longer in the DB schema

    @Column(name = "san_luong_tho", nullable = false, precision = 14, scale = 2)
    private BigDecimal sanLuongTho;

    @Column(name = "trang_thai_xu_ly", length = 20, nullable = false)
    private String trangThaiXuLy = "PENDING_MANUAL"; // VALIDATED, PENDING_MANUAL, TELEMETRY

    @Column(name = "loai_ghi_index", length = 20, nullable = false)
    private String loaiGhiIndex = "ORIGINAL"; // ORIGINAL, CORRECTION

    @Column(name = "id_chi_so_dieu_chinh")
    private Long idChiSoDieuChinh;

    @Column(name = "nguon_ghi", length = 20, nullable = false)
    private String nguonGhi = "AMR"; // AMR, HANDHELD, MANUAL

    @Column(name = "tgian_bdien", length = 10, nullable = false)
    private String tgianBdien = "BT"; // BT (Bình thường), CD (Cao điểm), TD (Thấp điểm)

    @Column(name = "ma_cto", length = 50)
    private String maCto;

    @Column(name = "so_lan_quay_vong", nullable = false)
    private Integer soLanQuayVong = 1;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    // Compatibility methods for existing codebase which queries getConsumption/setConsumption
    public BigDecimal getConsumption() {
        return sanLuongTho;
    }

    public void setConsumption(BigDecimal consumption) {
        this.sanLuongTho = consumption;
    }
}
