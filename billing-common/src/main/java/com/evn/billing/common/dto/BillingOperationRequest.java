package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BillingOperationRequest {
    private String loaiVanHanh;
    private String maKhang;
    private String dtuongQly;
    private String thangChuKy;
    private Integer kyChot;
}
