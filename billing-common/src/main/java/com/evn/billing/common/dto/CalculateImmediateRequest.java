package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CalculateImmediateRequest {
    private String maKhang;
    private String thangChuKy;
    private Integer kyChot = 1;
    private Integer phienBan = 1;
    private String dtuongQly = "SO_DEMAND";
    private String triggeredBy = "CMIS";
}
