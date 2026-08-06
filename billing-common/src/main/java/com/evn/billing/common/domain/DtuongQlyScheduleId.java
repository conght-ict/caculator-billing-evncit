package com.evn.billing.common.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DtuongQlyScheduleId implements Serializable {
    private String dtuongQly;
    private String thangCk;
    private Integer kyChot;
}
