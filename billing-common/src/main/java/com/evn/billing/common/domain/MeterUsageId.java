package com.evn.billing.common.domain;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MeterUsageId implements Serializable {
    private Long idChiSo;
    private Integer lanDocPhu;
    private String thangChuKy;
    private Integer kyChot;
}
