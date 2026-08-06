package com.evn.billing.common.domain;

import com.evn.billing.common.dto.MeterDetails;
import com.evn.billing.common.dto.TariffConfig;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.util.List;

@Entity
@Table(name = "diem_do")
@Data
public class MeterPoint {

    @Id
    @Column(name = "ma_ddo", length = 50)
    private String maDdo;

    @Column(name = "ma_khang", length = 50, nullable = false)
    private String maKhang;

    @Column(name = "dtuong_qly", length = 50, nullable = false)
    private String dtuongQly;

    @Column(name = "ma_capda", length = 20, nullable = false)
    private String maCapda;

    @Column(name = "trang_thai", length = 20, nullable = false)
    private String trangThai; // ACTIVE, INACTIVE

    @Column(name = "loai_ddo", nullable = false)
    private Short loaiDdo = 1;

    @Column(name = "loai_khang")
    private Short loaiKhang;

    @Column(name = "is_dien_mt", nullable = false)
    private Boolean isDienMt = false;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "thong_tin_cto", nullable = false)
    private List<MeterDetails> meterDetailsList;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "danh_sach_ap_gia", nullable = false)
    private List<TariffConfig> danhSachApGia;

    @Column(name = "ma_dviqly", length = 20, nullable = false)
    private String maDviqly = "PD0600";
}
