package com.evn.billing.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BillingConfigSnapshot {
    private String maKhang;
    private String dtuongQly;
    private String tenKhang;
    private String maSoThue;
    private String diaChi;
    private Short loaiKhang;
    // billingSchemaVersion: REMOVED — luôn null, không sử dụng
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
    // hasRelation: REMOVED — luôn = !fastPathEnabled, dư thừa 100%

    private String maDviqly; // Mã đơn vị quản lý
    private Integer phienBanTinh;


    // SAP IS-U Billing Schema steps
    private List<BillingSchemaStep> schemaSteps;

    // Compatibility methods for customerType
    @JsonIgnore
    public String getCustomerType() {
        return loaiKhangStr;
    }

    @JsonIgnore
    public void setCustomerType(String customerType) {
        this.loaiKhangStr = customerType;
    }

    @JsonIgnore
    public int getNormsFactor() {
        return soHo;
    }

    @JsonIgnore
    public void setNormsFactor(int normsFactor) {
        this.soHo = normsFactor;
    }

    @JsonIgnore
    public boolean isHasRelation() {
        return !fastPathEnabled;
    }
}
