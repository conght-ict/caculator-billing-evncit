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
import java.time.LocalDate;
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

    @Column(name = "so_tien", nullable = false, precision = 15, scale = 2)
    private BigDecimal soTien;

    @Column(name = "tien_gtgt", nullable = false, precision = 15, scale = 2)
    private BigDecimal tienGtgt;

    @Column(name = "tong_tien", nullable = false, precision = 15, scale = 2)
    private BigDecimal tongTien;

    @Column(name = "cosfi", precision = 8, scale = 3)
    private BigDecimal cosfi;

    @Column(name = "kcosfi", precision = 8, scale = 3)
    private BigDecimal kcosfi;

    @Column(name = "khoa_lap_trung", length = 200, nullable = false)
    private String khoaLapTrung;

    @Column(name = "cmis_id_hdon")
    private Long cmisIdHdon;

    @Column(name = "cmis_sync_status", length = 20, nullable = false)
    private String cmisSyncStatus = "PENDING";

    @Column(name = "cmis_sync_at")
    private LocalDateTime cmisSyncAt;

    @Column(name = "loai_hdon", length = 2, nullable = false)
    private String loaiHdon = "TD";

    @Column(name = "ngay_dky", nullable = false)
    private LocalDate ngayDky;

    @Column(name = "ngay_cky", nullable = false)
    private LocalDate ngayCky;

    @Column(name = "so_ho", precision = 8, scale = 2, nullable = false)
    private BigDecimal soHo = BigDecimal.ONE;

    @Column(name = "loai_khang", nullable = false)
    private Integer loaiKhang;

    @Column(name = "dien_tthu", precision = 15, scale = 2, nullable = false)
    private BigDecimal dienTthu;

    @Column(name = "tyle_thue", precision = 5, scale = 2, nullable = false)
    private BigDecimal tyleThue;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "chi_tiet_diem_do")
    private String chiTietDiemDo; // Stored as JSONB in DB

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
