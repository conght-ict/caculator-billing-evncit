package com.evn.billing.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BillingTaskDto {
    private String maKhang;
    private String dtuongQly;
    private String thangChuKy;
    private int kyChot;
    private int phienBanTinh;
    private String traceId;
    private List<MeterReadingDto> danhSachChiSo;
    private String triggeredBy;
    private int priority = 2; // 1 = BATCH, 2 = ROLLING

    private String loaiKhang; // SINH_HOAT, NGOAI_SINH_HOAT, MIXED
    private String changeFlags;      // NONE, PRICE_CHANGE, METER_CHANGE, READING_CHANGE, MULTI_CHANGE
    private boolean hasRelation;
    private Integer snapshotVersion;



    public BillingTaskDto(String maKhang, String dtuongQly, String thangChuKy, int kyChot, int phienBanTinh, String traceId) {
        this.maKhang = maKhang;
        this.dtuongQly = dtuongQly;
        this.thangChuKy = thangChuKy;
        this.kyChot = kyChot;
        this.phienBanTinh = phienBanTinh;
        this.traceId = traceId;
    }

    public BillingTaskDto(String maKhang, String dtuongQly, String thangChuKy, int kyChot, int phienBanTinh, String traceId, List<MeterReadingDto> danhSachChiSo) {
        this.maKhang = maKhang;
        this.dtuongQly = dtuongQly;
        this.thangChuKy = thangChuKy;
        this.kyChot = kyChot;
        this.phienBanTinh = phienBanTinh;
        this.traceId = traceId;
        this.danhSachChiSo = danhSachChiSo;
    }
}
