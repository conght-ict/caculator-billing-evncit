package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CalculationLogsRequest {
    private String dtuongQly;
    private String maKhang;
    private String trangThai;
    private Integer limit = 50;
}
