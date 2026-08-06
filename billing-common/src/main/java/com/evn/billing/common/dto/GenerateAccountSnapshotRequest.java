package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GenerateAccountSnapshotRequest {
    private String maKhang;
    private String thangChuKy;
    private Integer kyChot = 1;
    private String ruleId = "R-01";
    private String bangNguon = "diem_do";
    private String truongThayDoi = "danh_sach_ap_gia";
}
