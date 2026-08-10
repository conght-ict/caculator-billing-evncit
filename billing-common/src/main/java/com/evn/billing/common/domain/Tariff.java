package com.evn.billing.common.domain;

import com.evn.billing.common.dto.TariffBlock;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.math.BigDecimal;

@Entity
@Table(name = "bieu_gia")
@Data
public class Tariff {

    @Id
    @Column(name = "ma_bieu_gia", length = 100)
    private String maNgia;

    @Column(name = "ten_bieu_gia", length = 500)
    private String tenBieuGia;

    @Column(name = "loai_bieu_gia", length = 20, nullable = false)
    private String loaiBieuGia; // STEPPING, FLAT, TOU

    @Column(name = "ma_nhomnn", length = 20, nullable = false)
    private String maNhomnn;

    @Column(name = "khoang_da", length = 5)
    private String khoangDa;

    @Column(name = "ma_ngia_cmis", length = 10)
    private String maNgiaCmis;

    @Column(name = "thoigian_bdien", length = 5)
    private String thoigianBdien;

    @Column(name = "bac_thang", nullable = false)
    private boolean bacThang;

    @Column(name = "don_gia_phang")
    private BigDecimal donGiaPhang;

    @Column(name = "ngay_hieu_luc", nullable = false)
    private LocalDate ngayHieuLuc;

    @Column(name = "ngay_het_han")
    private LocalDate ngayHetHan;

    @Column(name = "quyet_dinh_phap_ly", length = 300)
    private String quyetDinhPhapLy;

    @Column(name = "trang_thai", length = 20)
    private String trangThai = "ACTIVE"; // ACTIVE, INACTIVE

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "chi_tiet_gia")
    private List<TariffBlock> blocks;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
}
