package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;

@Entity
@Table(name = "cau_hinh_thue")
@Data
public class CauHinhThue {
    @Id
    @Column(name = "loai_thue", length = 50)
    private String loaiThue; // e.g. "VAT"

    @Column(name = "thue_suat", nullable = false, precision = 5, scale = 4)
    private BigDecimal thueSuat;

    @Column(name = "ngay_adung", nullable = false)
    private LocalDate ngayAdung;
}
