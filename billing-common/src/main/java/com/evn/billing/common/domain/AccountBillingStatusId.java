package com.evn.billing.common.domain;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountBillingStatusId implements Serializable {
    private String maKhang;
    private String thangChuKy;
    private Integer kyChot;
}
