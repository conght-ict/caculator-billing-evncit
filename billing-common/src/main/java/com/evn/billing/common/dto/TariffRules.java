package com.evn.billing.common.dto;

import lombok.Data;
import java.util.List;

@Data
public class TariffRules {
    private String tariffCode;
    private String type; // STEPPING, FLAT, etc.
    private List<TariffBlock> blocks;
}
