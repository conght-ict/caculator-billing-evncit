package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MeterReadingDto {
    private String maDdo;
    private LocalDateTime tuNgay;
    private LocalDateTime denNgay;
    private BigDecimal chiSoDau;
    private BigDecimal chiSoCuoi;
    private BigDecimal sanLuong;
    private Boolean coQuayVong;
    private BigDecimal maxRegisterSnapshot;
    private Integer lanDocPhu;
    private String loaiGhiIndex;
    private String maCto;
    private Integer soLanQuayVong;
    private String tgianBdien; // tgian_bdien: BT, CD, TD, KT, VC

    public MeterReadingDto(String maDdo, LocalDateTime tuNgay, LocalDateTime denNgay,
                           BigDecimal chiSoDau, BigDecimal chiSoCuoi, BigDecimal sanLuong) {
        this.maDdo = maDdo;
        this.tuNgay = tuNgay;
        this.denNgay = denNgay;
        this.chiSoDau = chiSoDau;
        this.chiSoCuoi = chiSoCuoi;
        this.sanLuong = sanLuong;
        this.soLanQuayVong = 1;
    }
}
