package com.evn.billing.common.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

@Data
public class MeterDetails {
    private String soSeri;
    private String maCto;
    private BigDecimal heSoNhan = BigDecimal.ONE;
    private int soPha = 1;
    private List<String> danhSachBcs; // ["BT", "CD", "TD", "KT"]
    private LocalDate ngayTreo;
    private LocalDate ngayThao;
    private BigDecimal chiSoTreo;
    private BigDecimal chiSoThao;
    private String trangThai = "ACTIVE"; // ACTIVE, DECOMM
}
