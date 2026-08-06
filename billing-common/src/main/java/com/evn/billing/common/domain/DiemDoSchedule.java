package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "lich_ghi_ddo")
@IdClass(DiemDoScheduleId.class)
@Getter
@Setter
public class DiemDoSchedule {

    @Id
    @Column(name = "ma_ddo", length = 50)
    private String maDdo;

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

    @Column(name = "tthai_lich", length = 20, nullable = false)
    private String trangThai = "ACTIVE"; // tthai_lich

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt = LocalDateTime.now();
}
