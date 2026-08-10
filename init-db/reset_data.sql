-- =========================================================================
-- SCRIPT RESET TOÀN BỘ DỮ LIỆU TÍNH TOÁN CƯỚC VỀ TRẠNG THÁI BAN ĐẦU
-- Dành cho việc thử nghiệm & Phát triển (EVN Calculator Billing)
-- =========================================================================

-- 1. Dọn dẹp sạch hóa đơn và outbox event (bao gồm các bảng phân vùng)
TRUNCATE TABLE hoa_don RESTART IDENTITY CASCADE;
TRUNCATE TABLE su_kien_outbox RESTART IDENTITY CASCADE;

-- 2. Dọn dẹp snapshot cấu hình đã đóng băng
TRUNCATE TABLE snapshot_tinh_toan RESTART IDENTITY CASCADE;
TRUNCATE TABLE pending_snapshot_change RESTART IDENTITY CASCADE;

-- 3. Reset trạng thái tính toán của khách hàng về PENDING (mặc định ban đầu)
TRUNCATE TABLE trang_thai_tinh_toan_kh RESTART IDENTITY CASCADE;

-- Nạp lại dữ liệu khởi tạo cho trang_thai_tinh_toan_kh dựa trên danh sách khách hàng và lịch ghi đang hoạt động
INSERT INTO trang_thai_tinh_toan_kh (ma_khang, thang_chu_ky, dtuong_qly, ky_chot, trang_thai, so_lan_thu_lai)
SELECT DISTINCT d.ma_khang, l.thang_ck, d.dtuong_qly, l.ky_chot, 'PENDING', 0
FROM diem_do d
JOIN lich_ghi_dqly l ON d.dtuong_qly = l.dtuong_qly;

-- 4. Reset tiến độ chạy và bộ đếm trên bảng Lịch ghi chỉ số (lich_ghi_dqly)
UPDATE lich_ghi_dqly
SET tthai_chay = 'PENDING',
    kh_da_xl = 0,
    kh_tc = 0,
    kh_tb = 0;

-- 5. Cập nhật lại tổng số khách hàng thực tế cho từng sổ ghi để đảm bảo số liệu chuẩn
UPDATE lich_ghi_dqly l
SET tong_kh = COALESCE((
    SELECT COUNT(DISTINCT d.ma_khang)
    FROM diem_do d
    WHERE d.dtuong_qly = l.dtuong_qly
), 0);
