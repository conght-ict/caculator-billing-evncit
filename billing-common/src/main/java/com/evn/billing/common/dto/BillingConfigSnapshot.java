package com.evn.billing.common.dto;

import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class BillingConfigSnapshot {
    private String accountId;
    private int normsFactor;
    private MeterTopology meterTopology;
    private Map<String, TariffRules> tariffs; // Map of tariffCode -> TariffRules config
    
    // Fast Path optimization flags
    private boolean fastPathEnabled;
    private String fastPathMeterPointId;
    private String fastPathTariffCode;

    // SAP IS-U Billing Schema steps
    private List<BillingSchemaStep> schemaSteps;
}
