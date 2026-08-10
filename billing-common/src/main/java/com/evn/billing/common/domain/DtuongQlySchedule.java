package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "lich_ghi_dqly")
@IdClass(DtuongQlyScheduleId.class)
@Getter
@Setter
public class DtuongQlySchedule {

    @Id
    @Column(name = "dtuong_qly", length = 50)
    private String dtuongQly;

    @Id
    @Column(name = "thang_ck", length = 10)
    private String thangCk;

    @Id
    @Column(name = "ky_chot")
    private Integer kyChot = 1;

    @Column(name = "tu_ngay", nullable = false)
    private LocalDate tuNgay;

    @Column(name = "den_ngay", nullable = false)
    private LocalDate denNgay;

    @Column(name = "n_tru", nullable = false)
    private Integer nTru = 1; // n_tru

    @Column(name = "n_cong", nullable = false)
    private Integer nCong = 1;  // n_cong

    @Column(name = "tthai_lich", length = 20, nullable = false)
    private String trangThai = "ACTIVE"; // tthai_lich

    @Column(name = "tthai_chay", length = 20, nullable = false)
    private String trangThaiChay = "PENDING"; // tthai_chay

    @Column(name = "tong_kh")
    private int tongKh = 0; // tong_kh

    @Column(name = "kh_da_xl")
    private int khDaXl = 0; // kh_da_xl

    @Column(name = "kh_tc")
    private int khTc = 0; // kh_tc

    @Column(name = "kh_tb")
    private int khTb = 0; // kh_tb

    @Column(name = "nguon", length = 20)
    private String nguon = "CMIS"; // nguon

    @Column(name = "ma_dviqly", length = 20, nullable = false)
    private String maDviqly = "PD0600";

    @Column(name = "snapshot_generated", nullable = false)
    private Boolean snapshotGenerated = false;

    @Column(name = "snapshot_generated_at")
    private LocalDateTime snapshotGeneratedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt = LocalDateTime.now();
}
