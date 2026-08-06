package com.evn.billing.common.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DiemDoScheduleId implements Serializable {
    private String maDdo;
    private String thangCk;
    private Integer kyChot;
}
