package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "khach_hang")
@Getter
@Setter
public class Account {

    @Id
    @Column(name = "ma_khang", length = 50)
    private String maKhang;

    @Column(name = "ten_khang", length = 100, nullable = false)
    private String tenKhang;

    @Column(name = "trang_thai", length = 20, nullable = false)
    private String trangThai = "ACTIVE"; // ACTIVE, SUSPENDED

    @Column(name = "dia_chi")
    private String diaChi;

    @Column(name = "dien_thoai", length = 20)
    private String dienThoai;

    @Column(name = "ma_so_thue", length = 50)
    private String maSoThue;

    @Column(name = "email", length = 100)
    private String email;

    @Column(name = "ma_dviqly", length = 20, nullable = false)
    private String maDviqly = "PD0600";
}
