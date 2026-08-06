package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDate;

@Entity
@Table(name = "quan_he_diem_do")
@Data
public class MeterRelation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_quan_he")
    private Long idQuanHe;

    @Column(name = "ma_ddo_cha", length = 50, nullable = false)
    private String maDdoCha;

    @Column(name = "ma_ddo_con", length = 50, nullable = false)
    private String maDdoCon;

    @Column(name = "loai_quan_he", length = 20, nullable = false)
    private String loaiQuanHe; // AGGREGATION, NETTING

    @Column(name = "ngay_hieu_luc", nullable = false)
    private LocalDate ngayHieuLuc;

    @Column(name = "ngay_het_han")
    private LocalDate ngayHetHan;
}
