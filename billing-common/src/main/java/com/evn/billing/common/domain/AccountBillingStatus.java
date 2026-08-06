package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalDateTime;

@Entity
@Table(name = "trang_thai_tinh_toan_kh")
@IdClass(AccountBillingStatusId.class)
@Getter
@Setter
public class AccountBillingStatus {

    @Id
    @Column(name = "ma_khang", length = 50)
    private String maKhang;

    @Id
    @Column(name = "thang_chu_ky", length = 20)
    private String thangChuKy;

    @Id
    @Column(name = "ky_chot")
    private Integer kyChot = 1;

    @Column(name = "dtuong_qly", length = 50, nullable = false)
    private String dtuongQly;

    @Column(name = "trang_thai", length = 20, nullable = false)
    private String trangThai = "PENDING"; // PENDING, PROCESSING, SUCCESS, FAILED, DLQ

    @Column(name = "id_hoa_don", length = 100)
    private String idHoaDon;

    @Column(name = "thong_bao_loi")
    private String thongBaoLoi;

    @Column(name = "so_lan_thu_lai", nullable = false)
    private Integer soLanThuLai = 0;

    @Column(name = "thoi_gian_xu_ly_ms")
    private Long thoiGianXuLyMs;

    @Column(name = "ten_worker", length = 100)
    private String tenWorker;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt = LocalDateTime.now();
}
