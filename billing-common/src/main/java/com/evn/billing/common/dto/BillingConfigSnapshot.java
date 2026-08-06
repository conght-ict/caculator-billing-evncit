package com.evn.billing.common.dto;

import lombok.Data;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Data
public class BillingConfigSnapshot {
    private String maKhang;
    private String dtuongQly;
    private String tenKhang;
    private String maSoThue;
    private String diaChi;
    private Short loaiKhang;
    private String billingSchemaVersion;
    private int soHo;
    private String loaiKhangStr; // (Thay thế customerType cũ) SINH_HOAT, NGOAI_SINH_HOAT, MIXED
    private LocalDate ngayHieuLuc;
    private LocalDate tuNgay; // Target start date of the billing period
    private LocalDate denNgay;   // Target end date of the billing period
    private MeterTopology meterTopology;
    private Map<String, TariffRules> bieuGia; // Map of tariffCode -> TariffRules config
    
    // Fast Path optimization flags
    private boolean fastPathEnabled;
    private String fastPathMaDdo;
    private String fastPathMaNgia;

    private String changeFlags;      // NONE, PRICE_CHANGE, METER_CHANGE, MULTI_CHANGE
    private boolean hasRelation;

    private String maDviqly; // Mã đơn vị quản lý

    // SAP IS-U Billing Schema steps
    private List<BillingSchemaStep> schemaSteps;

    // Compatibility methods for customerType
    public String getCustomerType() {
        return loaiKhangStr;
    }

    public void setCustomerType(String customerType) {
        this.loaiKhangStr = customerType;
    }

    public int getNormsFactor() {
        return soHo;
    }

    public void setNormsFactor(int normsFactor) {
        this.soHo = normsFactor;
    }
}
