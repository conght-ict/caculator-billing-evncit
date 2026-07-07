package com.evn.billing.common.dto;

import lombok.Data;
import java.time.LocalDate;
import java.util.List;

@Data
public class TariffRules {
    private String tariffCode;
    private String type; // STEPPING, FLAT, TOU
    private LocalDate effectiveDate;
    private LocalDate expiryDate;
    private List<TariffBlock> blocks;
}
