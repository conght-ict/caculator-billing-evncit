package com.evn.billing.common.dto;

import lombok.Data;
import java.util.List;

@Data
public class MeterPointNode {
    private String meterPointId;
    private CalculationType calculationType;
    private String tariffCode; // Associated tariff for this meter point
    private List<MeterPointNode> childPoints;
}
