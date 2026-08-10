package com.evn.billing.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import java.time.LocalDate;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MeterPointNode {
    private String maDdo;
    private CalculationType calculationType;
    private String maNgia; // Default/primary tariff code (kept for backwards compatibility)
    private String meterSerial;
    private List<PriceApplicationRule> priceRules; // Freezing the bien_ban_ap_gia rules for this meter point
    private List<MeterPointNode> childPoints;
    private List<MeterDetails> activeMeters; // [MỚI] Các công tơ hoạt động trong kỳ tính toán
    private Short loaiDdo; // [MỚI] Loại điểm đo (phục vụ lấy bcs dự phòng)
    private Boolean isDienMt; // [MỚI] Xác định điểm đo có phải điện mặt trời hay không
    private LocalDate tuNgay;
    private LocalDate denNgay;
}
