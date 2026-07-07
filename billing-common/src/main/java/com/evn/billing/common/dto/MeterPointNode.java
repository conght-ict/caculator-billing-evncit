package com.evn.billing.common.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.util.List;

@Data
public class MeterPointNode {
    private String meterPointId;
    private CalculationType calculationType;
    private String tariffCode; // Associated tariff for this meter point
    private String meterSerial;
    private BigDecimal maxRegisterValue;
    private List<MeterPointNode> childPoints;
}
