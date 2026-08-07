package com.evn.billing.common.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class PriceApplicationRule {
    private String bbanId;
    private String maDdo;
    private int soThuTu; // so_thu_tu
    private BigDecimal dinhMuc; // dinh_muc
    private String loaiDmuc; // loai_dmuc (TL for percentage, SL for fixed consumption)
    private String loaiBcs; // loai_bcs (e.g. KT)
    private String tgianBdien; // tgian_bdien (BT, CD, TD)
    private String maNgia; // ma_ngia
    private int soHo; // so_ho (normsFactor for stepping)
    private String maCapda; // ma_capda
    private String maNhomnn;
    private boolean bacThang;
}
