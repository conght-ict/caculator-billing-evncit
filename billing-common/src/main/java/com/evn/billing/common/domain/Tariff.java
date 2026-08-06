package com.evn.billing.common.domain;

import com.evn.billing.common.dto.TariffBlock;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "bieu_gia")
@Data
public class Tariff {

    @Id
    @Column(name = "ma_bieu_gia", length = 50)
    private String maNgia;

    @Column(name = "ten_bieu_gia", length = 200, nullable = false)
    private String tenBieuGia;

    @Column(name = "loai_bieu_gia", length = 20, nullable = false)
    private String loaiBieuGia; // STEPPING, FLAT, TOU

    @Column(name = "ngay_hieu_luc", nullable = false)
    private LocalDate ngayHieuLuc;

    @Column(name = "ngay_het_han")
    private LocalDate ngayHetHan;

    @Column(name = "quyet_dinh_phap_ly", length = 300)
    private String quyetDinhPhapLy;

    @Column(name = "trang_thai", length = 20)
    private String trangThai = "ACTIVE"; // ACTIVE, INACTIVE

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "chi_tiet_gia", nullable = false)
    private List<TariffBlock> blocks;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
}
