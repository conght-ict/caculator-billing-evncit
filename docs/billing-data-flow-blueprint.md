# Bản thiết kế Luồng dữ liệu Tính cước & Ví dụ Thực tế (Billing Data Flow Blueprint & Examples)

Tài liệu này đặc tả chi tiết luồng chạy dữ liệu của hệ thống tính cước mới, cùng ví dụ nghiệp vụ áp giá thực tế theo cấu trúc Billing Schema kế thừa tiêu chuẩn SAP IS-U.

---

## 1. Các Luồng Dữ liệu Cốt lõi (Core Data Flows)

### Luồng A: Đóng băng Snapshot Cấu hình (Snapshot Freeze)
1. Khi có yêu cầu chốt sổ tháng, **`snapshot-generator`** truy vấn dữ liệu lưới điện từ các bảng quan hệ: `meter_point`, `meter_relation`, và `tariff_detail`.
2. Tạo cấu trúc cây đệ quy **`MeterTopology`** và danh sách các bước tính toán **`BillingSchemaStep`** (Ví dụ: tính cước $\rightarrow$ chiết khấu $\rightarrow$ tính thuế).
3. Đóng băng dữ liệu tĩnh này lưu vào bảng `billing_account_snapshot` và đồng bộ lên **Redis Cache** (TTL = 24h).

### Luồng B: Nhận Chỉ Số & Tính Cước Phản Ứng (Reactive Billing Flow)
1. CMIS nhận chỉ số chốt đo xa $\rightarrow$ bắn event `METER_READING_RECEIVED` vào Kafka topic `meter-readings-input`.
2. **`mediation-service`** nhận event, lưu vào bảng đệm `meter_usage` (trạng thái `VALIDATED`), rồi gọi **Completeness Check**.
3. Nếu đã nhận đủ chỉ số cho tất cả công tơ của khách hàng (đối chiếu qua Snapshot trên Redis) $\rightarrow$ Phát lệnh tính cước `BILLING_TASK_TRIGGER` sang Kafka topic `billing-execution-topic`.
4. **`billing-worker`** tiêu thụ task, đọc Snapshot từ Redis Cache, gọi thư viện **`rating-engine`** tính toán trực tiếp trên RAM: trừ phụ tải netting công tơ con $\rightarrow$ áp giá bậc thang $\rightarrow$ giảm giá $\rightarrow$ VAT.
5. Worker lưu kết quả hóa đơn (`BillInvoice`) và sự kiện gửi đi (`OutboxEvent`) vào CSDL trong một Transaction duy nhất.

### Luồng C: Đồng bộ khẩn cấp On-demand (Sync Fallback Flow)
1. Người dùng bấm xem hóa đơn trên CMIS $\rightarrow$ API `GET /api/v1/invoices` của `mediation-service` được gọi.
2. Nếu chưa có hóa đơn (404) $\rightarrow$ `mediation-service` gửi request REST nội bộ `POST /api/v1/billing/calculate-immediate` sang `billing-worker`.
3. Worker tính khẩn cấp bằng `rating-engine` trên RAM (<10ms) $\rightarrow$ ghi nhận hóa đơn vào DB $\rightarrow$ trả kết quả hiển thị tức thời cho CMIS.

### Luồng D: Đồng bộ ngược (Reverse CDC Sync)
1. Debezium CDC Connector phát hiện bản ghi mới trong bảng `outbox_event`.
2. Phát sự kiện `INVOICE_CREATED` vào Kafka để CMIS hoặc hệ thống hóa đơn điện tử tự động tiêu thụ phục vụ in ấn, gửi thông báo khách hàng.

---

## 2. Ví dụ Nghiệp vụ Tính toán Thực tế (Hộ hỗn hợp ghép công tơ)

### 2.1. Tham số Đầu vào của Khách hàng
* **Mã khách hàng**: `KH-MIX-001`.
* **Định mức hộ**: `normsFactor = 2` (2 hộ dùng chung công tơ).
* **Cơ cấu Lưới điện (Topology)**:
  - Công tơ Tổng (`METER-TONG`): Áp giá Sinh hoạt bậc thang `TARIFF_SHBT_2026`. Chỉ số tiêu thụ thô chốt kỳ: **500 kWh**.
  - Công tơ Phụ (`METER-PHU`): Đấu nối phía sau công tơ tổng đo điện kinh doanh. Áp giá phẳng `TARIFF_KDOANH_2026` (giá cố định 2,500đ/kWh). Chỉ số tiêu thụ thô chốt kỳ: **100 kWh**.
  - Quan hệ: Công tơ Phụ khấu trừ (`NETTING`) ra khỏi công tơ Tổng.
* **Chính sách áp dụng**: Trợ giá giảm trừ 10% cước trước thuế.

---

### 2.2. Trình tự Tính toán chi tiết của Rating Engine

#### Bước 1: Tính toán cây phụ tải (Topology Calculator)
* Công tơ con (`METER-PHU`): Nút lá thấp nhất $\rightarrow$ Sản lượng Net = **100 kWh**.
* Công tơ cha (`METER-TONG`):
$$\text{Sản lượng Net}_{\text{TONG}} = \text{Thô}_{\text{TONG}} - \text{Net}_{\text{con}} = 500 - 100 = 400\text{ kWh}$$

#### Bước 2: Chạy Bước 10 - Áp biểu giá bậc thang & phẳng (Rating Variant Step)

##### A. Tính toán công tơ con (`METER-PHU` - Áp giá FLAT TARIFF)
$$\text{Tiền điện công tơ con} = 100\text{ kWh} \times 2,500\text{đ} = 250,000\text{đ}$$

##### B. Tính toán công tơ cha (`METER-TONG` - Áp giá STEPPING TARIFF)
Do định mức hộ `normsFactor = 2`, giới hạn tối đa của các bậc thang cơ bản được nhân đôi ($Limit = Limit_{base} \times 2$):
* **Bậc 1 (giá 1,806đ)**: Định mức tối đa $50 \times 2 = 100\text{ kWh}$. Điện tiêu thụ rơi vào bậc = 100 kWh.
  $$\text{Tiền bậc 1} = 100 \times 1,806 = 180,600\text{đ}$$
* **Bậc 2 (giá 1,866đ)**: Định mức tối đa $50 \times 2 = 100\text{ kWh}$. Điện tiêu thụ rơi vào bậc = 100 kWh.
  $$\text{Tiền bậc 2} = 100 \times 1,866 = 186,600\text{đ}$$
* **Bậc 3 (giá 2,167đ)**: Định mức tối đa $100 \times 2 = 200\text{ kWh}$. Điện tiêu thụ rơi vào bậc = 200 kWh.
  $$\text{Tiền bậc 3} = 200 \times 2,167 = 433,400\text{đ}$$
* Tổng sản lượng đã phân bổ: $100 + 100 + 200 = 400\text{ kWh}$ (hết điện tiêu thụ).
* Tổng tiền điện công tơ tổng:
  $$\text{Tiền điện công tơ tổng} = 180,600 + 186,600 + 433,400 = 800,600\text{đ}$$

##### C. Tổng sản lượng trước thuế tại tài khoản (Aggregated Base)
$$\text{Tổng tiền trước thuế} (\text{BASE\_AMOUNT}) = 250,000 + 800,600 = 1,050,600\text{đ}$$

---

#### Bước 3: Chạy Bước 15 - Áp dụng chiết khấu trợ giá (Percent Discount Variant)
* Đầu vào: $\text{BASE\_AMOUNT} = 1,050,600\text{đ}$.
* Tham số chiết khấu: `10%` (0.10).
* Phép tính:
  $$\text{Tiền giảm trừ} (\text{DISCOUNT\_AMOUNT}) = 1,050,600 \times 0.10 = 105,060\text{đ}$$
  $$\text{Tiền sau giảm trừ} (\text{NET\_AMOUNT}) = 1,050,600 - 105,060 = 945,540\text{đ}$$

---

#### Bước 4: Chạy Bước 20 - Áp dụng thuế VAT (Tax Variant Step)
* Đầu vào: $\text{NET\_AMOUNT} = 945,540\text{đ}$.
* Thuế suất VAT: `10%` (0.10).
* Phép tính:
  $$\text{Tiền thuế VAT} (\text{TAX\_AMOUNT}) = 945,540 \times 0.10 = 94,554\text{đ}$$
  $$\text{Tổng thanh toán hóa đơn} (\text{TOTAL\_AMOUNT}) = 945,540 + 94,554 = 1,040,094\text{đ}$$

Hóa đơn xuất ra cuối cùng là **1,040,094đ**.
