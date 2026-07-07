package com.evn.billing.common.dto;

import lombok.Data;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Data
public class BillingConfigSnapshot {
    private String accountId;
    private String bookId;
    private int normsFactor;
    private LocalDate effectiveSyncDate;
    private LocalDate periodFromDate; // Target start date of the billing period
    private LocalDate periodToDate;   // Target end date of the billing period
    private MeterTopology meterTopology;
    private Map<String, TariffRules> tariffs; // Map of tariffCode -> TariffRules config
    
    // Fast Path optimization flags
    private boolean fastPathEnabled;
    private String fastPathMeterPointId;
    private String fastPathTariffCode;

    // SAP IS-U Billing Schema steps
    private List<BillingSchemaStep> schemaSteps;
}
