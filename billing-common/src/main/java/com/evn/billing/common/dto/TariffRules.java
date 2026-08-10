package com.evn.billing.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TariffRules {
    // maNgia: REMOVED — trùng với Map key trong bieuGia{}
    private String loaiBieuGia; // STEPPING, FLAT, TOU
    private boolean bacThang;
    private java.math.BigDecimal donGiaPhang;
    // ngayHieuLuc: REMOVED — đã filter tại snapshot generation time
    // ngayHetHan: REMOVED — đã filter tại snapshot generation time
    private List<TariffBlock> blocks;
}
