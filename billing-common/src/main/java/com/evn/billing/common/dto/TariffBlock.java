package com.evn.billing.common.dto;

import lombok.Data;

@Data
public class TariffBlock {
    private int step;
    private double minKwh;
    private Double maxKwh; // Can be null for the last tier
    private double unitPrice;
    private String touPeriod; // PEAK, OFF_PEAK, NORMAL
}
