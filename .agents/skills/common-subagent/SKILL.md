---
name: common-subagent
description: 'Use when: xử lý lớp dùng chung trong billing-common như DTO, Entity, Helper, Event schema, serialization, backward compatibility, và chuẩn BigDecimal.'
argument-hint: 'Nêu rõ class hoặc package trong billing-common và mục tiêu thay đổi'
user-invocable: true
disable-model-invocation: false
---

# 🤖 COMMON SUBAGENT DIRECTIVES

## When to Use
- Khi yêu cầu tập trung vào module `billing-common`.
- Khi cần tạo/sửa DTO, Entity, Value Object, helper dùng chung liên module.
- Khi cần chuẩn hóa schema event dùng chung hoặc tương thích ngược dữ liệu.

## Procedure
1. Xác định phạm vi thay đổi chỉ nằm trong `billing-common`.
2. Kiểm tra dependency để tránh import ngược từ module nghiệp vụ.
3. Thiết kế dữ liệu theo quy chuẩn composite key, JSON mapping, BigDecimal.
4. Đánh giá backward compatibility trước khi đổi schema DTO/event.
5. Đề xuất test hoặc migration liên quan nếu có thay đổi cấu trúc dữ liệu.

## 🏷️ Self-Identity Assertion
Khi Skill này được kích hoạt, câu đầu tiên trong phản hồi của bạn BẮT BUỘC phải bắt đầu bằng:
`[ACTIVE SUBAGENT: Common Subagent | Scope: billing-common]`

## 📌 Phạm vi dự án (Scope Boundary)
- `e:/caculator-billing-evncit/billing-common`

## ⚙️ Cổng MCP Sử Dụng
- Bắt buộc dùng MCP `billing-common` và `codegraph-cross-repo`.

## 🛡️ Nguyên tắc cốt lõi (Core Guidelines)

### 0. Phân tầng kiến trúc bắt buộc (Layered Architecture Rule)
*   **Controller**: Chỉ nhận request → gọi Service → trả response. **KHÔNG** chứa logic nghiệp vụ.
*   **Service**: Chứa TOÀN BỘ logic nghiệp vụ và orchestration.
*   **Repository**: Chỉ chứa thao tác dữ liệu. Không chứa logic nghiệp vụ.
*   Module `billing-common` chỉ chứa DTO/Entity/Helper dùng chung — KHÔNG chứa Controller hay Service.

### 1. Độc lập và Cấm Phụ Thuộc Ngược (Zero Downward Dependencies)
*   `billing-common` là module nền tảng được import bởi tất cả các module khác trong hệ thống.
*   **Tuyệt đối không** import bất kỳ class nào từ các module nghiệp vụ như `rating-engine`, `mediation-service`, hay `billing-worker` vào `billing-common`.
*   Tránh tạo ra các dependency vòng tròn (circular dependency).

### 2. Thiết kế DTOs & Entities cho Dữ liệu lớn (Big Data Serialization)
*   **Composite Keys**: Với các bảng phân vùng theo chu kỳ thời gian (ví dụ: `chi_so_dien_nang`, `trang_thai_tinh_toan_kh`), bắt buộc sử dụng `@IdClass` (như `MeterUsageId`) để định nghĩa khóa chính phức hợp gồm các trường phân vùng (như `ma_khang`, `thang_chu_ky`, `ky_chot`, `lan_doc_phu`).
*   **JSONB Mapping**: 
    *   Các trường lưu trữ cấu hình mềm hoặc cấu trúc đệ quy (như `thong_tin_cto`, `danh_sach_ap_gia` trong `DiemDo`, hoặc `du_lieu_cau_hinh` trong `BillingAccountSnapshot`) phải được map thành kiểu dữ liệu JSONB hoặc String/JSON thô dưới DB.
    *   Sử dụng Jackson `@JsonProperty` để ánh xạ cấu trúc JSON chính xác với DTOs.
*   **Tương thích ngược (Backward Compatibility)**:
    *   Khi sửa đổi cấu trúc DTOs hoặc Event Schema gửi lên Kafka, đảm bảo các trường mới có giá trị mặc định và không xóa hoặc đổi tên các trường cũ để tránh gây lỗi cho các Consumer chạy phiên bản cũ.

### 3. Quy chuẩn BigDecimal cho Dữ liệu tài chính
*   Tất cả các trường sản lượng (`sanLuong`, `sanLuongTho`), chỉ số (`chiSoDau`, `chiSoCuoi`), đơn giá, tiền điện trước thuế, tiền thuế, và tổng tiền sau thuế bắt buộc phải sử dụng kiểu dữ liệu **`java.math.BigDecimal`** trong Entity và DTO.
*   Không sử dụng `double` hoặc `float` dưới mọi hình thức để tránh sai số dấu phẩy động trong nghiệp vụ kế toán của EVN.

### 4. Không dùng giá trị mặc định khi thiếu dữ liệu trọng yếu
*   Khi thiết kế, refactor DTO/Entity hay các lớp Helper trong `billing-common`, cấm tự ý gán giá trị mặc định (ví dụ: gán cứng `vatRate = 0.10`, `normsFactor = 1`, `tgianBdien = "KT"`) nếu cấu hình hoặc dữ liệu đầu vào bị thiếu.
*   Nếu dữ liệu hoặc cấu hình thiếu và ảnh hưởng đến tính đúng đắn của luồng tính toán cước, bắt buộc phải báo lỗi/ném Exception thích hợp để các tầng xử lý trên ghi nhận log và đối soát thủ công.

