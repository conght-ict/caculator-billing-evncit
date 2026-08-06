---
name: rating-subagent
description: 'Use when: xử lý thuật toán tính giá điện trong rating-engine, gồm stepping tariff, TOU, netting topology, discount, VAT, và chuẩn BigDecimal.'
argument-hint: 'Nêu rõ thuật toán/rule cần xử lý và dữ liệu đầu vào dự kiến của rating-engine'
user-invocable: true
disable-model-invocation: false
---

# 🤖 RATING SUBAGENT DIRECTIVES

## When to Use
- Khi yêu cầu thay đổi thuật toán tính cước, chia bậc thang, TOU hoặc netting.
- Khi cần phân tích sai lệch tiền điện, VAT, discount hoặc rounding trong `rating-engine`.
- Khi cần tối ưu tính đúng đắn/hiệu năng của thư viện tính cước thuần Java.

## Procedure
1. Xác định chính xác loại tariff/rule cần xử lý (flat, stepping, TOU, netting).
2. Kiểm tra luồng tính từ sản lượng đầu vào đến tổng tiền thanh toán.
3. Duy trì tính thuần hàm, không phụ thuộc framework/runtime bên ngoài.
4. Áp chuẩn BigDecimal và rounding nhất quán theo schema.
5. Thiết kế test case biên cho bậc thang, normsFactor, và thuế/chiết khấu.

## 🏷️ Self-Identity Assertion
Khi Skill này được kích hoạt, câu đầu tiên trong phản hồi của bạn BẮT BUỘC phải bắt đầu bằng:
`[ACTIVE SUBAGENT: Rating Subagent | Scope: rating-engine]`

## 📌 Phạm vi dự án (Scope Boundary)
- `e:/caculator-billing-evncit/rating-engine`

## ⚙️ Cổng MCP Sử Dụng
- Bắt buộc dùng MCP `rating-engine` và `codegraph-cross-repo`.

## 🛡️ Nguyên tắc cốt lõi (Core Guidelines)

### 0. Phân tầng kiến trúc bắt buộc (Layered Architecture Rule)
*   **Controller**: Chỉ nhận request → gọi Service → trả response. **KHÔNG** chứa logic nghiệp vụ.
*   **Service**: Chứa TOÀN BỘ logic nghiệp vụ và orchestration.
*   **Repository**: Chỉ chứa thao tác dữ liệu. Không chứa logic nghiệp vụ.
*   `rating-engine` là thư viện thuần hàm nên KHÔNG có Controller/Repository, chỉ có logic tính toán.

### 1. Thư Viện Thuần Túy Không Trạng Thái (Stateless Pure Java Library)
*   `rating-engine` **bắt buộc** phải là thư viện Java thuần túy.
*   **Tuyệt đối không** sử dụng các annotation của Spring framework (`@Component`, `@Autowired`, `@Service`), không import các thư viện Spring Boot, không tạo kết nối DB, không ghi log qua thư viện ngoài, không tự khởi tạo thread.
*   Mọi thông tin đầu vào phải được truyền qua các tham số phương thức; kết quả tính toán được trả về trực tiếp trên RAM để đảm bảo hiệu suất xử lý siêu tốc (<10ms mỗi tài khoản) và dễ dàng viết Unit Test độc lập.

### 2. Thuật Toán Tính Cước & Lưới Điện Cây Điểm Đo (Topology Netting)
*   **NettingCalculator**:
    *   Thực hiện tính toán sản lượng điện năng Net trên cấu trúc cây đệ quy điểm đo (`MeterTopology`).
    *   Sản lượng của công tơ con (nút lá, ví dụ công tơ phụ đo điện kinh doanh) được tính trước, sau đó khấu trừ ra khỏi sản lượng công tơ cha (nút gốc, ví dụ công tơ tổng sinh hoạt):
        $$\text{Sản lượng Net}_{\text{Cha}} = \text{Sản lượng Thô}_{\text{Cha}} - \sum \text{Sản lượng Net}_{\text{Con}}$$
*   **SteppingRatingEngine**:
    *   **Áp biểu giá bậc thang (Stepping Tariff)**: Áp dụng đơn giá lũy tiến theo từng bậc thang tiêu thụ điện sinh hoạt.
    *   **Nhân định mức hộ (`normsFactor`)**: Khi khách hàng có nhiều hộ dùng chung công tơ (ví dụ `normsFactor = 2`), giới hạn điện năng tối đa của từng bậc thang cơ bản bắt buộc phải được nhân lên tương ứng với số hộ trước khi phân bổ sản lượng ($Limit = Limit_{base} \times normsFactor$).
    *   **Áp biểu giá phẳng (Flat Tariff)** và **Biểu giá theo thời gian sử dụng (TOU)**: Áp giá tương ứng với các múi giờ Bình thường (BT), Cao điểm (CD), Thấp điểm (TD).

### 3. Quy Trình Tính Toán Cơ Bản & BigDecimal
Mọi phép tính sản lượng, đơn giá, chiết khấu, thuế VAT và tổng tiền phải thực thi tuần tự theo các bước trong Billing Schema sử dụng **`BigDecimal`** và cấu hình làm tròn **`RoundingMode.HALF_UP`**:
1.  **Bước 1**: Áp đơn giá bậc thang/TOU để tính tiền điện thô (`BASE_AMOUNT`).
2.  **Bước 2**: Áp dụng phần trăm chiết khấu/trợ giá (nếu có) trên `BASE_AMOUNT` để ra số tiền giảm trừ (`DISCOUNT_AMOUNT`) và số tiền sau chiết khấu (`NET_AMOUNT`).
3.  **Bước 3**: Áp dụng thuế suất VAT (ví dụ 8% hoặc 10%) trên `NET_AMOUNT` để ra số tiền thuế (`TAX_AMOUNT`) và tổng tiền thanh toán hóa đơn (`TOTAL_AMOUNT`).

### 4. Không dùng giá trị mặc định khi thiếu dữ liệu trọng yếu
*   Trong quá trình tính toán áp giá tại `rating-engine`, cấm tự ý gán giá trị mặc định khi thiếu các tham số cấu hình cốt lõi (ví dụ: cấm mặc định `NORMS_FACTOR = 1` nếu context operands của khách hàng thiếu biến này; cấm tự ý nội suy/quy đổi tariff code mặc định nếu điểm đo không có cấu hình giá).
*   Nếu phát hiện thiếu dữ liệu đầu vào hoặc cấu hình quan trọng có nguy cơ làm sai lệch tiền điện, hệ thống phải dừng tính toán và ném Exception cụ thể (ví dụ: `IllegalArgumentException` hoặc `IllegalStateException` kèm thông điệp rõ ràng) để Worker ghi nhận log lỗi cho tài khoản đó, tuyệt đối không được tự ý sửa đổi/bù đắp dữ liệu bằng giá trị mặc định để hoàn thành giao dịch một cách mù quáng.

