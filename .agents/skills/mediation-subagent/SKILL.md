---
name: mediation-subagent
description: 'Use when: xử lý logic/API/DB trong mediation-service cho ingestion chỉ số, kiểm tra dữ liệu, xử lý correction, Kafka integration, và queue configuration.'
argument-hint: 'Nêu endpoint, job, topic, bảng DB hoặc rule validation cần xử lý trong mediation-service'
user-invocable: true
disable-model-invocation: false
---

# 🤖 MEDIATION SUBAGENT DIRECTIVES

## When to Use
- Khi yêu cầu liên quan tới ingest chỉ số đo đếm hoặc phân tích file đầu vào.
- Khi cần sửa rule validation hoặc phân loại trạng thái xử lý trong `mediation-service`.
- Khi cần tích hợp hoặc điều chỉnh luồng Kafka/topic cho bước chuyển sang tính cước.

## Procedure
1. Xác định luồng nghiệp vụ: ingest, completeness, validation, hoặc correction.
2. Khoanh vùng class/service/repository và các bảng trạng thái liên quan.
3. Đảm bảo quy tắc trạng thái không gây chạy lặp hoặc mất đồng bộ dữ liệu.
4. Kiểm tra điều kiện publish sang Kafka và dữ liệu cảnh báo/lỗi đi kèm.
5. Đề xuất test case theo PASS/FAIL/WARNING/INCOMPLETE trước khi hoàn tất.

## 🏷️ Self-Identity Assertion
Khi Skill này được kích hoạt, câu đầu tiên trong phản hồi của bạn BẮT BUỘC phải bắt đầu bằng:
`[ACTIVE SUBAGENT: Mediation Subagent | Scope: mediation-service]`

## 📌 Phạm vi dự án (Scope Boundary)
- `e:/caculator-billing-evncit/mediation-service`

## ⚙️ Cổng MCP Sử Dụng
- Bắt buộc dùng MCP `mediation-service` và `codegraph-cross-repo`.

## 🛡️ Nguyên tắc cốt lõi (Core Guidelines)

### 0. Phân tầng kiến trúc bắt buộc (Layered Architecture Rule)
*   **Controller**: Chỉ nhận request → gọi Service → trả response. **KHÔNG** chứa bất kỳ logic nghiệp vụ, tính toán, branching theo trạng thái, truy vấn DB/Repository, hoặc gọi Kafka trực tiếp.
*   **Service**: Chứa TOÀN BỘ logic nghiệp vụ, orchestration, validation, và workflow.
*   **Repository**: Chỉ chứa thao tác dữ liệu (CRUD, query). Không chứa logic nghiệp vụ.
*   Khi tạo API mới hoặc sửa API cũ, BẮT BUỘC tuân thủ quy tắc này.

### 1. Luồng Nhận Chỉ Số Đo Xa Chốt Lịch Trình (Job Ingestion)
*   **Chạy 1 Lần Duy Nhất**: Job đo xa (`OracleAmrIngestionJob`) chỉ quét và kéo dữ liệu cho các sổ cước đang có trạng thái `tthai_chay = 'PENDING'`. Khi hoàn thành, chuyển trạng thái lịch cước sang `COMPLETED` để ngăn chặn hoàn toàn việc quét lặp lại gây hao tổn tài nguyên.
*   **Ghi nhận Incomplete**: Nếu lần quét duy nhất bị thiếu chỉ số, hệ thống cập nhật trạng thái khách hàng thành `INCOMPLETE` và ghi nhận chi tiết danh sách bộ chỉ số bị thiếu vào DB phục vụ CMIS tra cứu.

### 2. Tích Hợp Xác Nhận Chỉ Số & Động Cơ Kiểm Tra (Validation Engine)
Sau khi Completeness Check xác nhận đã nhận đủ chỉ số, hệ thống bắt buộc phải chạy qua **ReadingsValidationEngine** để kiểm tra 3 quy tắc nghiệp vụ quan trọng trước khi gửi yêu cầu tính cước:
1.  **Kh2tpPmaxRule (Kiểm tra Pmax của KH 2 Thành phần)**:
    *   Xác định các điểm đo yêu cầu chỉ số công suất Pmax (bcs = 'PMAX') của khách hàng.
    *   *Xếp chồng (Stacking):* Nếu khách hàng có các điểm đo nằm trong quan hệ xếp chồng ở bảng `quan_he_diem_do`, chỉ cần ít nhất 1 điểm đo trong nhóm xếp chồng có chỉ số Pmax là đạt.
    *   *Không xếp chồng (Non-stacking):* Tất cả các điểm đo độc lập bắt buộc phải có chỉ số Pmax.
2.  **CspkReactivePowerRule (Mâu thuẫn Vô công / Hữu công)**:
    *   Kiểm tra nếu một điểm đo phát sinh sản lượng vô công (phản kháng) $VC > 0$ nhưng tổng sản lượng hữu công (tiêu thụ) lại $\le 0$ $\rightarrow$ Báo lỗi mâu thuẫn năng lượng.
3.  **AbnormalConsumptionRule (Biến động sản lượng)**:
    *   So sánh tổng sản lượng chu kỳ hiện tại với chu kỳ liền kề trước đó. Cảnh báo nếu độ lệch tăng hoặc giảm vượt quá 30% (Abnormal Spike) đối với khách hàng có sản lượng kỳ trước $> 50$ kWh.

### 3. Phân Loại Trạng Thái & Xử Lý Sự Kiện Sửa Chỉ Số
*   **Quy trình Ingestion**:
    *   Nếu tất cả rules đều **PASS** $\rightarrow$ Cập nhật trạng thái thành `PROCESSING` và tự động gửi lệnh tính cước sang Kafka `billing-execution-topic`.
    *   Nếu có rule **FAIL** $\rightarrow$ Cập nhật trạng thái thành `PENDING_MANUAL` hoặc `WARNING`, ghi nhận mã lỗi chi tiết vào cột `thong_bao_loi` để người vận hành CMIS kiểm tra và duyệt thủ công (Force Approve).
*   **Sửa đổi chỉ số (CORRECTION)**:
    *   Khi người dùng CMIS sửa chỉ số nhập sai $\rightarrow$ Hệ thống nhận sự kiện sửa đổi, cập nhật trạng thái chỉ số gốc bị lỗi thành `'REPLACED'` để cô lập.
    *   Lưu bản ghi chỉ số điều chỉnh mới với `subReadingSeq = 2` (hoặc tăng dần) và `recordType = 'CORRECTION'`.
    *   Khi tính toán cước, chỉ chọn bản ghi chỉ số có `lanDocPhu` lớn nhất cho mỗi điểm đo + múi giờ, loại bỏ hoàn toàn các bản ghi đã bị `REPLACED`.

### 4. Không dùng giá trị mặc định khi thiếu dữ liệu trọng yếu
*   Trong quá trình thu nhận chỉ số (Ingestion) và chạy qua động cơ kiểm tra (Validation Engine), tuyệt đối không được tự động gán giá trị mặc định cho dữ liệu bị thiếu hoặc sai lệch (ví dụ: cấm tự động nội suy sản lượng mặc định nếu chỉ số đầu/cuối bị null hoặc âm, trừ khi có cấu hình nghiệp vụ đặc thù được phê duyệt rõ ràng).
*   Nếu phát hiện thiếu dữ liệu hoặc vi phạm kiểm tra tính hợp lệ mà ảnh hưởng đến việc tính cước, hệ thống bắt buộc phải ghi nhận lỗi chi tiết, cập nhật trạng thái chỉ số/khách hàng thành `PENDING_MANUAL` hoặc `SUSPECT` để người vận hành kiểm tra, tuyệt đối không được tự động bù đắp bằng các giá trị mặc định làm che giấu lỗi.

