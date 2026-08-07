package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "hoa_don")
@IdClass(BillInvoiceId.class)
@Getter
@Setter
public class BillInvoice {

    @Id
    @Column(name = "id_hoa_don", length = 100)
    private String idHoaDon;

    @Id
    @Column(name = "thang_chu_ky", length = 20)
    private String thangChuKy; // Format: YYYY_MM

    @Column(name = "ma_khang", length = 50, nullable = false)
    private String maKhang;

    @Column(name = "dtuong_qly", length = 50, nullable = false)
    private String dtuongQly;

    @Column(name = "ky_chot", nullable = false)
    private Integer kyChot = 1;

    @Column(name = "tong_tien_truoc_thue", nullable = false, precision = 15, scale = 2)
    private BigDecimal tongTienTruocThue;

    @Column(name = "tien_thue", nullable = false, precision = 15, scale = 2)
    private BigDecimal tienThue;

    @Column(name = "tong_tien_sau_thue", nullable = false, precision = 15, scale = 2)
    private BigDecimal tongTienSauThue;

    @Column(name = "khoa_lap_trung", length = 200, nullable = false)
    private String khoaLapTrung;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "ban_ke_tinh_toan", nullable = false)
    private String banKeTinhToan; // Stored as JSONB in DB

    @Column(name = "ap_dung_phan_bo", nullable = false)
    private Boolean apDungPhanBo = false;

    @Column(name = "ref_snapshot", length = 200)
    private String refSnapshot;

    @Column(name = "trang_thai_tinh_toan", length = 20, nullable = false)
    private String trangThaiTinhToan = "FINAL"; // FINAL, RECALCULATED, DISPUTED

    @Column(name = "ma_dviqly", length = 20, nullable = false)
    private String maDviqly = "PD0600";

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt = LocalDateTime.now();
}
