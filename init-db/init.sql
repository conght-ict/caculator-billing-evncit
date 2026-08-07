-- =========================================================================
-- DATABASE INITIALIZATION SCRIPT v5.0 (Vietnamese Schema - JSONB Optimized)
-- EVN National Billing System — Optimized Schema aligned with CMIS Conventions
-- =========================================================================

-- 1. Khách Hàng (Tối giản các trường phi tính toán)
CREATE TABLE khach_hang (
    ma_khang            VARCHAR(50) PRIMARY KEY,
    ten_khang           VARCHAR(200) NOT NULL,
    trang_thai          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | SUSPENDED
    dia_chi             TEXT,
    dien_thoai          VARCHAR(50),
    email               VARCHAR(100),
    ma_so_thue          VARCHAR(50),
    ma_dviqly           VARCHAR(20) NOT NULL DEFAULT 'PD0600', -- [MỚI] Mã đơn vị quản lý
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Lịch Ghi Đối Tượng Quản Lý (lich_ghi_dqly)
CREATE TABLE lich_ghi_dqly (
    dtuong_qly          VARCHAR(50) NOT NULL,
    thang_ck            VARCHAR(10) NOT NULL, -- Định dạng: YYYY_MM (ví dụ: '2026_06')
    ky_chot             INT NOT NULL DEFAULT 1, -- Kỳ thứ mấy trong tháng (1, 2, 3...)
    tu_ngay             DATE NOT NULL, -- Ngày bắt đầu kỳ cước
    den_ngay            DATE NOT NULL, -- Ngày chốt kỳ cước
    n_tru               INT NOT NULL DEFAULT 1, -- Số ngày cho phép ghi sớm (N-1)
    n_cong              INT NOT NULL DEFAULT 1, -- Số ngày cho phép ghi muộn (N+1)
    ma_dviqly           VARCHAR(20) NOT NULL DEFAULT 'PD0600', -- [MỚI] Mã đơn vị quản lý
    
    tthai_lich          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | CLOSED
    tthai_chay          VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING | PROCESSING | COMPLETED | FAILED
    
    tong_kh             INT DEFAULT 0,
    kh_da_xl            INT DEFAULT 0,
    kh_tc               INT DEFAULT 0,
    kh_tb               INT DEFAULT 0,
    nguon               VARCHAR(20) DEFAULT 'CMIS',
    
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dtuong_qly, thang_ck, ky_chot),
    CONSTRAINT chk_lich_dqly_dates CHECK (den_ngay >= tu_ngay)
);

-- 3. Điểm Đo (Tối ưu hóa ZERO JOIN: Công tơ và Áp giá dạng JSONB)
CREATE TABLE dm_loai_ddo (
    loai_ddo    SMALLINT NOT NULL,
    loai_bcs    VARCHAR(5) NOT NULL,
    mo_ta       VARCHAR(100),
    PRIMARY KEY (loai_ddo, loai_bcs)
);

INSERT INTO dm_loai_ddo (loai_ddo, loai_bcs, mo_ta) VALUES
(1, 'KT', '1 giá - Tổng'),
(2, 'BT', '2 giá - Bình thường'), (2, 'TD', '2 giá - Thấp điểm'),
(3, 'BT', '3 giá - Bình thường'), (3, 'CD', '3 giá - Cao điểm'), (3, 'TD', '3 giá - Thấp điểm'),
(4, 'KT', '1 giá + VC'), (4, 'VC', '1 giá + VC'),
(5, 'BT', '2 giá + VC'), (5, 'TD', '2 giá + VC'), (5, 'VC', '2 giá + VC'),
(6, 'BT', '3 giá + VC'), (6, 'CD', '3 giá + VC'), (6, 'TD', '3 giá + VC'), (6, 'VC', '3 giá + VC');

CREATE TABLE diem_do (
    ma_ddo              VARCHAR(50) PRIMARY KEY,
    ma_khang            VARCHAR(50) NOT NULL REFERENCES khach_hang(ma_khang),
    dtuong_qly          VARCHAR(50) NOT NULL, -- Sổ ghi chỉ số chốt cước
    ma_capda            VARCHAR(20) NOT NULL, -- HẠ ÁP | TRUNG ÁP | CAO ÁP
    trang_thai          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | INACTIVE
    loai_ddo            SMALLINT NOT NULL DEFAULT 1,
    loai_khang          SMALLINT,
    is_dien_mt          BOOLEAN NOT NULL DEFAULT FALSE,
    thong_tin_cto       JSONB NOT NULL DEFAULT '[]'::jsonb, -- Danh sách công tơ treo tháo dạng JSONB Array
    danh_sach_ap_gia    JSONB NOT NULL,       -- Mảng các đối tượng áp giá (ma_nhomnn, ma_nn, ma_ngia, tgian_bdien...)
    ma_dviqly           VARCHAR(20) NOT NULL DEFAULT 'PD0600', -- [MỚI] Mã đơn vị quản lý
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_diem_do_khang ON diem_do(ma_khang);
CREATE INDEX idx_diem_do_dqly ON diem_do(dtuong_qly);
CREATE INDEX idx_diem_do_ap_gia ON diem_do USING GIN (danh_sach_ap_gia);

-- 3b. Lịch Ghi Điểm Đo (lich_ghi_ddo)
CREATE TABLE lich_ghi_ddo (
    ma_ddo              VARCHAR(50) NOT NULL REFERENCES diem_do(ma_ddo),
    thang_ck            VARCHAR(10) NOT NULL, -- Định dạng: YYYY_MM
    ky_chot             INT NOT NULL DEFAULT 1,
    tu_ngay             DATE NOT NULL,
    den_ngay            DATE NOT NULL,
    tthai_lich          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | CLOSED
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ma_ddo, thang_ck, ky_chot),
    CONSTRAINT chk_lich_ddo_dates CHECK (den_ngay >= tu_ngay)
);

-- 4. Quan Hệ Điểm Đo (Biểu diễn cây công tơ phụ tải netting)
CREATE TABLE quan_he_diem_do (
    id_quan_he          BIGSERIAL PRIMARY KEY,
    ma_ddo_cha          VARCHAR(50) NOT NULL REFERENCES diem_do(ma_ddo),
    ma_ddo_con          VARCHAR(50) NOT NULL REFERENCES diem_do(ma_ddo),
    loai_quan_he        VARCHAR(20) DEFAULT 'NETTING',
    ngay_hieu_luc       DATE NOT NULL DEFAULT CURRENT_DATE,
    ngay_het_han        DATE,
    CONSTRAINT chk_different_meters CHECK (ma_ddo_cha <> ma_ddo_con)
);
CREATE INDEX idx_qh_ddo_cha ON quan_he_diem_do(ma_ddo_cha);

-- 5. Cấu Hình Biểu Giá (Lưu các bậc thang dạng JSONB để nạp O(1) Memory Cache)
CREATE TABLE bieu_gia (
    ma_bieu_gia         VARCHAR(100) PRIMARY KEY,
    ten_bieu_gia        VARCHAR(500),
    loai_bieu_gia       VARCHAR(20) NOT NULL, -- STEPPING | FLAT | TOU
    ma_nhomnn           VARCHAR(20) NOT NULL,
    khoang_da           VARCHAR(5),
    ma_ngia_cmis        VARCHAR(10),
    thoigian_bdien      VARCHAR(5),
    bac_thang           BOOLEAN NOT NULL DEFAULT FALSE,
    don_gia_phang       DECIMAL(15,2),
    ngay_hieu_luc       DATE NOT NULL,
    ngay_het_han        DATE,
    quyet_dinh_phap_ly  VARCHAR(300),
    chi_tiet_gia        JSONB, -- Mảng các bậc thang [TariffBlock] (step, minKwh, maxKwh, unitPrice...)
    trang_thai          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_bieu_gia_cmis_mapping ON bieu_gia(ma_nhomnn, khoang_da, ma_ngia_cmis, thoigian_bdien);
CREATE INDEX idx_bieu_gia_temporal ON bieu_gia(loai_bieu_gia, ngay_hieu_luc, ngay_het_han);

-- 5b. Cấu hình Hệ Số Cosphi
CREATE TABLE IF NOT EXISTS cau_hinh_cosfi (
    hs_cosfi            DECIMAL(3,2) PRIMARY KEY,
    kcosfi              DECIMAL(5,2) NOT NULL,
    ngay_adung          DATE NOT NULL
);

-- 5c. Tỷ Giá Ngoại Tệ Quy Đổi
CREATE TABLE IF NOT EXISTS ty_gia (
    ma_dviqly           VARCHAR(6) NOT NULL,
    id_ty_gia           INT NOT NULL,
    loai_tien           VARCHAR(5) NOT NULL,
    tygia_qdoi          DECIMAL(10,2) NOT NULL,
    thang               INT NOT NULL,
    nam                 INT NOT NULL,
    ngay_nhap           TIMESTAMP NOT NULL,
    PRIMARY KEY (ma_dviqly, loai_tien, thang, nam)
);

-- 6. Chỉ Số Điện Năng (Áp dụng Partition theo thang_chu_ky)
CREATE TABLE chi_so_dien_nang (
    id_chi_so               BIGINT NOT NULL,
    lan_doc_phu             INT NOT NULL DEFAULT 1, -- Thứ tự thay thế công tơ giữa chu kỳ

    ma_khang                VARCHAR(50) NOT NULL,
    ma_ddo                  VARCHAR(50) NOT NULL,
    thang_chu_ky            VARCHAR(20) NOT NULL,
    ky_chot                 INT NOT NULL DEFAULT 1,

    tu_ngay                 TIMESTAMP NOT NULL,
    den_ngay                TIMESTAMP NOT NULL,
    CONSTRAINT chk_cs_dates CHECK (den_ngay > tu_ngay),

    chi_so_dau              DECIMAL(14,2) NOT NULL,
    chi_so_cuoi             DECIMAL(14,2) NOT NULL,

    co_quay_vong            BOOLEAN NOT NULL DEFAULT FALSE,
    san_luong_tho           DECIMAL(14,2) NOT NULL,

    trang_thai_xu_ly        VARCHAR(20) NOT NULL DEFAULT 'PENDING_MANUAL', -- PENDING_MANUAL | VALIDATED | TELEMETRY

    loai_ghi_index          VARCHAR(20) NOT NULL DEFAULT 'ORIGINAL', -- ORIGINAL | CORRECTION
    id_chi_so_dieu_chinh    BIGINT,

    nguon_ghi               VARCHAR(20) NOT NULL DEFAULT 'AMR', -- AMR | HANDHELD | MANUAL
    tgian_bdien             VARCHAR(10) NOT NULL DEFAULT 'BT', -- BT (Bình thường) | CD | TD
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id_chi_so, lan_doc_phu, thang_chu_ky)
) PARTITION BY LIST (thang_chu_ky);

-- Đảm bảo không trùng khoảng thời gian ghi của cùng điểm đo
CREATE UNIQUE INDEX uq_chi_so_dien_nang_no_overlap
    ON chi_so_dien_nang (ma_ddo, tu_ngay, den_ngay, thang_chu_ky);

-- Các bảng phân vùng (Partitions) cho chi_so_dien_nang
CREATE TABLE chi_so_dien_nang_2026_06 PARTITION OF chi_so_dien_nang FOR VALUES IN ('2026_06');
CREATE TABLE chi_so_dien_nang_2026_07 PARTITION OF chi_so_dien_nang FOR VALUES IN ('2026_07');
CREATE TABLE chi_so_dien_nang_2026_08 PARTITION OF chi_so_dien_nang FOR VALUES IN ('2026_08');
CREATE TABLE chi_so_dien_nang_default PARTITION OF chi_so_dien_nang DEFAULT;

CREATE INDEX idx_cs_dien_nang_lookup ON chi_so_dien_nang(ma_khang, thang_chu_ky, trang_thai_xu_ly);
CREATE INDEX idx_cs_dien_nang_ddo    ON chi_so_dien_nang(ma_ddo, thang_chu_ky);

-- 7. Snapshot Tính Toán (Materialized Snapshots)
CREATE TABLE snapshot_tinh_toan (
    id_snapshot             VARCHAR(200) PRIMARY KEY, -- Định dạng: {ma_khang}_{thang_chu_ky}_p{ky_chot}_v{phien_ban_tinh}
    ma_khang                VARCHAR(50) NOT NULL,
    dtuong_qly              VARCHAR(50) NOT NULL,
    thang_chu_ky            VARCHAR(20) NOT NULL,
    ky_chot                 INT NOT NULL DEFAULT 1,
    phien_ban_tinh          INT NOT NULL DEFAULT 1,
    trang_thai              VARCHAR(20) NOT NULL DEFAULT 'DRAFT', -- DRAFT | LOCKED | DEPRECATED
    phien_ban_luat_cuoc     VARCHAR(50) NOT NULL DEFAULT '2026.08',
    ma_dviqly               VARCHAR(20) NOT NULL DEFAULT 'PD0600', -- [MỚI] Mã đơn vị quản lý

    ngay_dong_bo_hieu_luc   DATE NOT NULL,
    du_lieu_cau_hinh        JSONB NOT NULL, -- Chứa cấu hình đóng băng bao gồm cả cây điểm đo và các biên bản áp giá
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Trigger function to enforce Snapshot Lock Rule
CREATE OR REPLACE FUNCTION tg_prevent_locked_snapshot_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF (OLD.trang_thai = 'LOCKED') THEN
        RAISE EXCEPTION 'Cannot modify or delete a LOCKED snapshot (id_snapshot = %). Ensure snapshot is isolation-guaranteed.', OLD.id_snapshot;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_locked_snapshot_mutation
BEFORE UPDATE OR DELETE ON snapshot_tinh_toan
FOR EACH ROW EXECUTE FUNCTION tg_prevent_locked_snapshot_mutation();

CREATE UNIQUE INDEX idx_snapshot_tinh_toan_composite
    ON snapshot_tinh_toan(ma_khang, thang_chu_ky, ky_chot, phien_ban_tinh);
CREATE INDEX idx_snapshot_tinh_toan_dqly
    ON snapshot_tinh_toan(dtuong_qly, thang_chu_ky, ky_chot);
CREATE INDEX idx_snapshot_tinh_toan_jsonb
    ON snapshot_tinh_toan USING GIN (du_lieu_cau_hinh);

-- 8. Hóa Đơn (Tích hợp chi tiết tính cước thành cột JSONB - Phân vùng theo thang_chu_ky)
CREATE TABLE hoa_don (
    id_hoa_don              VARCHAR(100) NOT NULL,
    ma_khang                VARCHAR(50) NOT NULL,
    dtuong_qly              VARCHAR(50) NOT NULL,
    thang_chu_ky            VARCHAR(20) NOT NULL,
    ky_chot                 INT NOT NULL DEFAULT 1,
    tong_tien_truoc_thue    DECIMAL(15,2) NOT NULL,
    tien_thue               DECIMAL(15,2) NOT NULL,
    tong_tien_sau_thue      DECIMAL(15,2) NOT NULL,
    ma_dviqly               VARCHAR(20) NOT NULL DEFAULT 'PD0600', -- [MỚI] Mã đơn vị quản lý

    khoa_lap_trung           VARCHAR(200) NOT NULL, -- idempotency_key
    ban_ke_tinh_toan        JSONB NOT NULL, -- Manifest chi tiết bậc thang, thuế, phân bổ
    ap_dung_phan_bo         BOOLEAN NOT NULL DEFAULT FALSE,
    ref_snapshot            VARCHAR(200),
    trang_thai_tinh_toan    VARCHAR(20) NOT NULL DEFAULT 'FINAL',

    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id_hoa_don, thang_chu_ky),
    CONSTRAINT uq_idempotency_hoa_don UNIQUE (khoa_lap_trung, thang_chu_ky)
) PARTITION BY LIST (thang_chu_ky);

-- Các bảng phân vùng cho hoa_don
CREATE TABLE hoa_don_2026_06 PARTITION OF hoa_don FOR VALUES IN ('2026_06');
CREATE TABLE hoa_don_2026_07 PARTITION OF hoa_don FOR VALUES IN ('2026_07');
CREATE TABLE hoa_don_2026_08 PARTITION OF hoa_don FOR VALUES IN ('2026_08');
CREATE TABLE hoa_don_default PARTITION OF hoa_don DEFAULT;

CREATE INDEX idx_hoa_don_khang ON hoa_don(ma_khang, thang_chu_ky);
CREATE INDEX idx_hoa_don_dqly ON hoa_don(dtuong_qly, thang_chu_ky);

-- 9. Sự Kiện Outbox
CREATE TABLE su_kien_outbox (
    id_su_kien      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    loai_doi_tuong  VARCHAR(50) NOT NULL, -- aggregate_type
    id_doi_tuong    VARCHAR(100) NOT NULL, -- aggregate_id
    loai_su_kien    VARCHAR(50) NOT NULL, -- event_type
    noi_dung        JSONB NOT NULL, -- payload
    trang_thai      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX uq_outbox_business_event
    ON su_kien_outbox(loai_doi_tuong, id_doi_tuong, loai_su_kien);
CREATE INDEX idx_outbox_pending ON su_kien_outbox(trang_thai, created_at) WHERE trang_thai = 'PENDING';

-- 10. Trạng Thái Tính Toán Khách Hàng (Theo dõi trạng thái chốt tổng quát)
CREATE TABLE trang_thai_tinh_toan_kh (
    ma_khang                VARCHAR(50) NOT NULL,
    thang_chu_ky            VARCHAR(20) NOT NULL,
    dtuong_qly              VARCHAR(50) NOT NULL,
    ky_chot                 INT NOT NULL DEFAULT 1,
    trang_thai              VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING | PROCESSING | SUCCESS | FAILED
    id_hoa_don              VARCHAR(100),
    thong_bao_loi           TEXT,
    so_lan_thu_lai          INT DEFAULT 0,
    thoi_gian_xu_ly_ms      BIGINT,
    ten_worker              VARCHAR(100),
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ma_khang, thang_chu_ky, ky_chot)
) PARTITION BY LIST (thang_chu_ky);

CREATE TABLE trang_thai_tinh_toan_kh_2026_06 PARTITION OF trang_thai_tinh_toan_kh FOR VALUES IN ('2026_06');
CREATE TABLE trang_thai_tinh_toan_kh_2026_07 PARTITION OF trang_thai_tinh_toan_kh FOR VALUES IN ('2026_07');
CREATE TABLE trang_thai_tinh_toan_kh_2026_08 PARTITION OF trang_thai_tinh_toan_kh FOR VALUES IN ('2026_08');
CREATE TABLE trang_thai_tinh_toan_kh_default PARTITION OF trang_thai_tinh_toan_kh DEFAULT;

CREATE INDEX idx_trang_thai_tinh_toan_kh_dqly ON trang_thai_tinh_toan_kh(dtuong_qly, thang_chu_ky, trang_thai);



-- 11b. Thay Đổi Cấu Hình Đang Chờ Xử Lý (pending_snapshot_change)
CREATE TABLE pending_snapshot_change (
    id_thay_doi         BIGSERIAL PRIMARY KEY,
    ma_khang            VARCHAR(50) NOT NULL,
    thang_chu_ky        VARCHAR(20) NOT NULL,
    ky_chot             INT NOT NULL DEFAULT 1,
    rule_id             VARCHAR(10) NOT NULL,
    bang_nguon          VARCHAR(50) NOT NULL,
    truong_thay_doi     TEXT NOT NULL,
    du_lieu_cu          JSONB,
    du_lieu_moi         JSONB,
    trang_thai          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    grace_expires_at    TIMESTAMP NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at        TIMESTAMP
);
CREATE INDEX idx_pending_change_expire ON pending_snapshot_change(trang_thai, grace_expires_at) WHERE trang_thai = 'PENDING';
CREATE INDEX idx_pending_change_khang ON pending_snapshot_change(ma_khang, thang_chu_ky, ky_chot);


-- =========================================================================
-- SEED DATA - Dữ liệu giả lập chạy thử
-- =========================================================================

-- Biểu Giá (Tariffs - Tích hợp TariffBlock JSONB)
INSERT INTO bieu_gia (ma_bieu_gia, ten_bieu_gia, loai_bieu_gia, ngay_hieu_luc, quyet_dinh_phap_ly, chi_tiet_gia) VALUES
('TARIFF_SHBT_2023', 'Sinh hoạt bậc thang 2023', 'STEPPING', '2023-05-04', 'QD 648/QD-BCT 20/03/2023', 
 '[{"step": 1, "minKwh": 0, "maxKwh": 50, "unitPrice": 1806.00}, 
   {"step": 2, "minKwh": 50, "maxKwh": 100, "unitPrice": 1866.00}, 
   {"step": 3, "minKwh": 100, "maxKwh": 200, "unitPrice": 2167.00}, 
   {"step": 4, "minKwh": 200, "maxKwh": 300, "unitPrice": 2729.00}, 
   {"step": 5, "minKwh": 300, "maxKwh": 400, "unitPrice": 3050.00}, 
   {"step": 6, "minKwh": 400, "maxKwh": null, "unitPrice": 3157.00}]'::jsonb),
('TARIFF_KDOANH_2023', 'Kinh doanh đồng giá 2023', 'FLAT', '2023-05-04', 'QD 648/QD-BCT 20/03/2023', 
 '[{"step": 1, "minKwh": 0, "maxKwh": null, "unitPrice": 2500.00}]'::jsonb),
('TARIFF_SX_2023', 'Sản xuất đồng giá 2023', 'FLAT', '2023-05-04', 'QD 648/QD-BCT 20/03/2023', 
 '[{"step": 1, "minKwh": 0, "maxKwh": null, "unitPrice": 2000.00}]'::jsonb),
('TARIFF_SX_BT', 'Sản xuất - Giờ Bình thường (TOU)', 'TOU', '2023-05-04', 'QD 648/QD-BCT 20/03/2023', 
 '[{"step": 1, "minKwh": 0, "maxKwh": null, "unitPrice": 1800.00, "touPeriod": "NORMAL"}]'::jsonb),
('TARIFF_SX_CD', 'Sản xuất - Giờ Cao điểm (TOU)', 'TOU', '2023-05-04', 'QD 648/QD-BCT 20/03/2023', 
 '[{"step": 1, "minKwh": 0, "maxKwh": null, "unitPrice": 3200.00, "touPeriod": "PEAK"}]'::jsonb),
('TARIFF_SX_TD', 'Sản xuất - Giờ Thấp điểm (TOU)', 'TOU', '2023-05-04', 'QD 648/QD-BCT 20/03/2023', 
 '[{"step": 1, "minKwh": 0, "maxKwh": null, "unitPrice": 1100.00, "touPeriod": "OFF_PEAK"}]'::jsonb);

-- Lịch Ghi Đối Tượng Quản Lý (Schedules)
INSERT INTO lich_ghi_dqly (dtuong_qly, thang_ck, ky_chot, tu_ngay, den_ngay, tthai_lich, ma_dviqly) VALUES
('SO_01', '2026_06', 1, '2026-06-01', '2026-06-10', 'CLOSED', 'PD0100'),
('SO_01', '2026_06', 2, '2026-06-11', '2026-06-20', 'CLOSED', 'PD0100'),
('SO_01', '2026_06', 3, '2026-06-21', '2026-06-30', 'ACTIVE', 'PD0100'),
('SO_01', '2026_07', 1, '2026-07-01', '2026-07-31', 'ACTIVE', 'PD0100');

-- Khách Hàng (Tối giản)
INSERT INTO khach_hang (ma_khang, ten_khang, dia_chi, dien_thoai, ma_so_thue, email, ma_dviqly) VALUES
('KH001', 'Nguyen Van A (Sinh hoạt 1 hộ)', '123 Đường Láng, Hà Nội', '0912345678', '0102030405', 'a.nguyen@gmail.com', 'PD0100'),
('KH002', 'Tran Thi B (Sản xuất đơn giá)', '456 Phố Vọng, Hà Nội', '0987654321', '0203040506', 'b.tran@gmail.com', 'PA1100'),
('KH003', 'Công ty C (Hỗn hợp + Netting + 3 hộ)', '789 Đường Bưởi, Hà Nội', '0901234567', '0304050607', 'c.company@gmail.com', 'PD0100'),
('KH005', 'Nhà máy E (Sản xuất TOU 3 giá)', 'KCN Thăng Long, Đông Anh', '0243123456', '0506070809', 'e.factory@gmail.com', 'PA1100'),
('KH006', 'Hộ F (Mô phỏng quay vòng chỉ số)', '12 Ngõ Trại, Hà Nội', '0955555555', '0607080910', 'f.rollover@gmail.com', 'PD0100');

-- Điểm Đo (Lồng ghép thong_tin_cto và danh_sach_ap_gia JSONB)
INSERT INTO diem_do (ma_ddo, ma_khang, dtuong_qly, ma_capda, trang_thai, loai_ddo, loai_khang, is_dien_mt, thong_tin_cto, danh_sach_ap_gia, ma_dviqly) VALUES
('METER-01', 'KH001', 'SO_01', 'HẠ ÁP', 'ACTIVE', 1, 1, FALSE,
 '[{"so_seri": "SN-11111", "ma_cto": "SN-11111", "he_so_nhan": 1.0, "so_pha": 1, "danh_sach_bcs": ["KT"], "ngay_treo": "2025-01-01", "ngay_thao": null, "trang_thai": "ACTIVE"}]'::jsonb,
 '[{"soThuTu": 1, "maNhomnn": "SHBT", "maNn": "4401", "maCapda": "1", "maNgia": "TARIFF_SHBT_2023", "tgianBdien": "BT", "dinhMuc": 100.00, "loaiDmuc": "TL", "soHo": 1}]'::jsonb, 'PD0100'),

('METER-02', 'KH002', 'SO_01', 'TRUNG ÁP', 'ACTIVE', 1, 2, FALSE,
 '[{"so_seri": "SN-22222", "ma_cto": "SN-22222", "he_so_nhan": 1.0, "so_pha": 3, "danh_sach_bcs": ["KT"], "ngay_treo": "2025-01-01", "ngay_thao": null, "trang_thai": "ACTIVE"}]'::jsonb,
 '[{"soThuTu": 1, "maNhomnn": "SXBT", "maNn": "2201", "maCapda": "2", "maNgia": "TARIFF_SX_2023", "tgianBdien": "BT", "dinhMuc": 100.00, "loaiDmuc": "TL", "soHo": 0}]'::jsonb, 'PA1100'),
 
('METER-03-TONG', 'KH003', 'SO_01', 'HẠ ÁP', 'ACTIVE', 1, 1, FALSE,
 '[{"so_seri": "SN-33300", "ma_cto": "SN-33300", "he_so_nhan": 1.0, "so_pha": 3, "danh_sach_bcs": ["KT"], "ngay_treo": "2025-01-01", "ngay_thao": null, "trang_thai": "ACTIVE"}]'::jsonb,
 '[{"soThuTu": 1, "maNhomnn": "SHBT", "maNn": "4401", "maCapda": "1", "maNgia": "TARIFF_SHBT_2023", "tgianBdien": "BT", "dinhMuc": 70.00, "loaiDmuc": "TL", "soHo": 3},
   {"soThuTu": 2, "maNhomnn": "KDDV", "maNn": "3101", "maCapda": "1", "maNgia": "TARIFF_KDOANH_2023", "tgianBdien": "BT", "dinhMuc": 30.00, "loaiDmuc": "TL", "soHo": 1}]'::jsonb, 'PD0100'),
   
('METER-03-PHU', 'KH003', 'SO_01', 'HẠ ÁP', 'ACTIVE', 1, 1, FALSE,
 '[{"so_seri": "SN-33301", "ma_cto": "SN-33301", "he_so_nhan": 1.0, "so_pha": 1, "danh_sach_bcs": ["KT"], "ngay_treo": "2025-01-01", "ngay_thao": null, "trang_thai": "ACTIVE"}]'::jsonb,
 '[{"soThuTu": 1, "maNhomnn": "KDDV", "maNn": "3101", "maCapda": "1", "maNgia": "TARIFF_KDOANH_2023", "tgianBdien": "BT", "dinhMuc": 100.00, "loaiDmuc": "TL", "soHo": 1}]'::jsonb, 'PD0100'),
 
('METER-05-BT', 'KH005', 'SO_01', 'TRUNG ÁP', 'ACTIVE', 2, 2, FALSE,
 '[{"so_seri": "SN-55551", "ma_cto": "SN-55551", "he_so_nhan": 1.0, "so_pha": 3, "danh_sach_bcs": ["BT"], "ngay_treo": "2025-01-01", "ngay_thao": null, "trang_thai": "ACTIVE"}]'::jsonb,
 '[{"soThuTu": 1, "maNhomnn": "SXBT", "maNn": "2201", "maCapda": "2", "maNgia": "TARIFF_SX_BT", "tgianBdien": "BT", "dinhMuc": 100.00, "loaiDmuc": "TL", "soHo": 0}]'::jsonb, 'PA1100'),
 
('METER-05-CD', 'KH005', 'SO_01', 'TRUNG ÁP', 'ACTIVE', 3, 2, FALSE,
 '[{"so_seri": "SN-55552", "ma_cto": "SN-55552", "he_so_nhan": 1.0, "so_pha": 3, "danh_sach_bcs": ["CD"], "ngay_treo": "2025-01-01", "ngay_thao": null, "trang_thai": "ACTIVE"}]'::jsonb,
 '[{"soThuTu": 1, "maNhomnn": "SXBT", "maNn": "2201", "maCapda": "2", "maNgia": "TARIFF_SX_CD", "tgianBdien": "CD", "dinhMuc": 100.00, "loaiDmuc": "TL", "soHo": 0}]'::jsonb, 'PA1100'),
 
('METER-05-TD', 'KH005', 'SO_01', 'TRUNG ÁP', 'ACTIVE', 3, 2, FALSE,
 '[{"so_seri": "SN-55553", "ma_cto": "SN-55553", "he_so_nhan": 1.0, "so_pha": 3, "danh_sach_bcs": ["TD"], "ngay_treo": "2025-01-01", "ngay_thao": null, "trang_thai": "ACTIVE"}]'::jsonb,
 '[{"soThuTu": 1, "maNhomnn": "SXBT", "maNn": "2201", "maCapda": "2", "maNgia": "TARIFF_SX_TD", "tgianBdien": "TD", "dinhMuc": 100.00, "loaiDmuc": "TL", "soHo": 0}]'::jsonb, 'PA1100'),
 
('METER-06', 'KH006', 'SO_01', 'HẠ ÁP', 'ACTIVE', 1, 1, FALSE,
 '[{"so_seri": "SN-66666", "ma_cto": "SN-66666", "he_so_nhan": 1.0, "so_pha": 1, "danh_sach_bcs": ["KT"], "ngay_treo": "2025-01-01", "ngay_thao": null, "trang_thai": "ACTIVE"}]'::jsonb,
 '[{"soThuTu": 1, "maNhomnn": "SHBT", "maNn": "4401", "maCapda": "1", "maNgia": "TARIFF_SHBT_2023", "tgianBdien": "BT", "dinhMuc": 100.00, "loaiDmuc": "TL", "soHo": 1}]'::jsonb, 'PD0100');

-- Quan Hệ Điểm Đo (Topology Netting)
INSERT INTO quan_he_diem_do (ma_ddo_cha, ma_ddo_con, loai_quan_he, ngay_hieu_luc) VALUES
('METER-03-TONG', 'METER-03-PHU', 'NETTING', '2020-01-01');

-- Chỉ Số Điện Năng — Chu kỳ 2026_06 (VALIDATED)
INSERT INTO chi_so_dien_nang (
    id_chi_so, lan_doc_phu, ma_khang, ma_ddo, thang_chu_ky, ky_chot,
    tu_ngay, den_ngay, chi_so_dau, chi_so_cuoi,
    co_quay_vong, san_luong_tho,
    trang_thai_xu_ly, loai_ghi_index, nguon_ghi, tgian_bdien
) VALUES
-- KH001: 250 kWh tiêu chuẩn
(1, 1, 'KH001', 'METER-01', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 1000.00, 1250.00, FALSE, 250.00, 'VALIDATED', 'ORIGINAL', 'AMR', 'BT'),
 
-- KH002: 10000 kWh sản xuất
(2, 1, 'KH002', 'METER-02', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 5000.00, 15000.00, FALSE, 10000.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD', 'BT'),
 
-- KH003: Netting — TONG=500kWh, PHU=100kWh -> Net = 400kWh
(3, 1, 'KH003', 'METER-03-TONG', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 2000.00, 2500.00, FALSE, 500.00, 'VALIDATED', 'ORIGINAL', 'AMR', 'BT'),
(4, 1, 'KH003', 'METER-03-PHU', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 500.00, 600.00, FALSE, 100.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD', 'BT'),
 
-- KH005: TOU 3 giá (BT=1000, CD=200, TD=500 kWh)
(8, 1, 'KH005', 'METER-05-BT', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 1000.00, 2000.00, FALSE, 1000.00, 'VALIDATED', 'ORIGINAL', 'AMR', 'BT'),
(9, 1, 'KH005', 'METER-05-CD', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 500.00, 700.00, FALSE, 200.00, 'VALIDATED', 'ORIGINAL', 'AMR', 'CD'),
(10, 1, 'KH005', 'METER-05-TD', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 300.00, 800.00, FALSE, 500.00, 'VALIDATED', 'ORIGINAL', 'AMR', 'TD'),
 
-- KH006: Quay vòng số của công tơ cơ
(11, 1, 'KH006', 'METER-06', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 99900.00, 100.00, TRUE, 200.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD', 'BT');

-- Chỉ Số Điện Năng — Thay công tơ giữa chu kỳ của KH001 tháng 07
INSERT INTO chi_so_dien_nang (
    id_chi_so, lan_doc_phu, ma_khang, ma_ddo, thang_chu_ky, ky_chot,
    tu_ngay, den_ngay, chi_so_dau, chi_so_cuoi,
    co_quay_vong, san_luong_tho,
    trang_thai_xu_ly, loai_ghi_index, nguon_ghi, tgian_bdien
) VALUES
(12, 1, 'KH001', 'METER-01', '2026_07', 1,
 '2026-07-01 00:00:00', '2026-07-10 23:59:59',
 1250.00, 1300.00, FALSE, 50.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD', 'BT'),
(13, 2, 'KH001', 'METER-01', '2026_07', 1,
 '2026-07-10 00:00:00', '2026-07-31 23:59:59',
 0.00, 200.00, FALSE, 200.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD', 'BT');

-- 11. Nhật Ký Lỗi Tính Toán (Fail-Safe Logging)
CREATE TABLE nhat_ky_loi_tinh_toan (
    id_nhat_ky              BIGSERIAL PRIMARY KEY,
    ma_khang                VARCHAR(50) NOT NULL,
    thang_chu_ky            VARCHAR(20) NOT NULL,
    ky_chot                 INT NOT NULL,
    loai_loi                VARCHAR(100) NOT NULL, -- SNAPSHOT_MALFORM | CALCULATION_ERROR | DB_ERROR
    chi_tiet_loi            TEXT NOT NULL,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_nhat_ky_loi_khang ON nhat_ky_loi_tinh_toan(ma_khang, thang_chu_ky);

-- 12. Lịch Xử Lý Lại (Self-Healing Scheduler)
CREATE TABLE lich_xu_ly_lai (
    id_nhiem_vu            BIGSERIAL PRIMARY KEY,
    ma_khang                VARCHAR(50) NOT NULL,
    thang_chu_ky            VARCHAR(20) NOT NULL,
    ky_chot                 INT NOT NULL,
    so_lan_thu_lai          INT NOT NULL DEFAULT 0,
    loi_cuoi_cung           TEXT,
    thoi_gian_thu_lai_ke    TIMESTAMP NOT NULL,
    trang_thai              VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING | COMPLETED | FAILED
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_lich_xu_ly_lai_hen ON lich_xu_ly_lai(trang_thai, thoi_gian_thu_lai_ke) WHERE trang_thai = 'PENDING';

-- 13. Nhật Ký Tính Toán (Lưu chi tiết các lần tính cước thành công/thất bại)
CREATE TABLE nhat_ky_tinh_toan (
    id_log          UUID PRIMARY KEY,
    dtuong_qly      VARCHAR(50) NOT NULL,
    ma_khang        VARCHAR(50) NOT NULL,
    thang_chu_ky    VARCHAR(20) NOT NULL,
    ky_chot         INT NOT NULL,
    trang_thai      VARCHAR(20) NOT NULL,
    du_lieu_vao     JSONB,
    du_lieu_ra      JSONB,
    thong_bao_loi   TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_nhat_ky_tinh_toan_lookup ON nhat_ky_tinh_toan(ma_khang, thang_chu_ky, ky_chot);

-- 14. Nhật Ký Chỉ Số (Lifecycle logging cho Ingestion/Validation - Partitioned)
CREATE TABLE nhat_ky_chi_so (
    id_log          UUID NOT NULL DEFAULT gen_random_uuid(),
    ma_khang        VARCHAR(50),
    ma_ddo          VARCHAR(50),
    thang_chu_ky    VARCHAR(20) NOT NULL,
    ky_chot         INT,
    buoc_xu_ly      VARCHAR(30),  -- INGESTION | VALIDATION | COMPLETENESS
    trang_thai      VARCHAR(20),  -- VALIDATED | SUSPECT | PENDING_MANUAL | TELEMETRY
    chi_tiet        JSONB,
    nguon_ghi       VARCHAR(20),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id_log, thang_chu_ky)
) PARTITION BY LIST (thang_chu_ky);

CREATE TABLE nhat_ky_chi_so_2026_06 PARTITION OF nhat_ky_chi_so FOR VALUES IN ('2026_06');
CREATE TABLE nhat_ky_chi_so_2026_07 PARTITION OF nhat_ky_chi_so FOR VALUES IN ('2026_07');
CREATE TABLE nhat_ky_chi_so_2026_08 PARTITION OF nhat_ky_chi_so FOR VALUES IN ('2026_08');
CREATE TABLE nhat_ky_chi_so_default PARTITION OF nhat_ky_chi_so DEFAULT;
CREATE INDEX idx_nhat_ky_chi_so_lookup ON nhat_ky_chi_so(ma_khang, thang_chu_ky);
