package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountsByStatusRequest {
    private String dtuongQly;
    private String thangChuKy;
    private Integer kyChot = 1;
    private String statuses;
    private Integer page = 0;
    private Integer size = 10;
}
