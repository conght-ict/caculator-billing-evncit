package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorLogsRequest {
    private String maKhang;
    private String thangChuKy;
    private Integer kyChot;
    private Integer limit = 50;
}
