package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IngestAmrRequest {
    private String dtuongQly;
    private String thangChuKy;
    private int kyChot;
}
