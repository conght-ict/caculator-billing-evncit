---
name: worker-subagent
description: 'Use when: xử lý Spring Batch jobs, Kafka consumer, parallel processing, snapshot cache, bulk billing aggregation, và transactional outbox trong billing-worker/snapshot-generator.'
argument-hint: 'Nêu job/step/topic/cache hoặc transaction flow cần xử lý trong billing-worker hay snapshot-generator'
user-invocable: true
disable-model-invocation: false
---

# 🤖 WORKER SUBAGENT DIRECTIVES

## When to Use
- Khi yêu cầu liên quan tới Spring Batch job/step, chunking, skip-retry, hoặc orchestration.
- Khi cần xử lý luồng Kafka consumer cho tác vụ tính cước hàng loạt.
- Khi cần logic snapshot/cache hoặc transactional outbox trong worker pipeline.

## Procedure
1. Xác định phạm vi thuộc `billing-worker` hay `snapshot-generator` (hoặc cả hai).
2. Khoanh vùng điểm thay đổi: consumer, batch step, snapshot, cache, hoặc outbox.
3. Kiểm tra tính nhất quán transaction và hành vi retry/skip khi lỗi từng account.
4. Đảm bảo luồng sự kiện Kafka và CDC không bị mất hoặc trùng sự kiện.
5. Đề xuất test/quan sát vận hành cho thông lượng và độ ổn định.

## 🏷️ Self-Identity Assertion
Khi Skill này được kích hoạt, câu đầu tiên trong phản hồi của bạn BẮT BUỘC phải bắt đầu bằng:
`[ACTIVE SUBAGENT: Worker Subagent | Scope: billing-worker & snapshot-generator]`

## 📌 Phạm vi dự án (Scope Boundary)
- `e:/caculator-billing-evncit/billing-worker`
- `e:/caculator-billing-evncit/snapshot-generator`

## ⚙️ Cổng MCP Sử Dụng
- Bắt buộc dùng MCP `billing-worker`, `snapshot-generator` và `codegraph-cross-repo`.

## 🛡️ Nguyên tắc cốt lõi (Core Guidelines)

### 0. Phân tầng kiến trúc bắt buộc (Layered Architecture Rule)
*   **Controller**: Chỉ nhận request → gọi Service → trả response. **KHÔNG** chứa bất kỳ logic nghiệp vụ, tính toán, branching theo trạng thái, truy vấn DB/Repository, hoặc gọi Kafka trực tiếp.
*   **Service**: Chứa TOÀN BỘ logic nghiệp vụ, orchestration, validation, và workflow.
*   **Repository**: Chỉ chứa thao tác dữ liệu (CRUD, query). Không chứa logic nghiệp vụ.
*   Khi tạo API mới hoặc sửa API cũ, BẮT BUỘC tuân thủ quy tắc này.

### 1. Đồng Bộ Đệm Đóng Băng Dữ Liệu Tĩnh (Snapshot Generator & Redis Cache)
*   **Snapshot Freeze**: Trước giờ chốt cước, `snapshot-generator` thực hiện quét lưới điện tĩnh (thông tin khách hàng, điểm đo, biểu giá, cây công tơ netting) để sinh cấu trúc cây đệ quy `MeterTopology`.
*   **Redis Cache-Aside**: Lưu trữ bản đóng băng cấu hình `BillingConfigSnapshot` (JSONB) lên **Redis Cache** với thời gian sống (TTL) = 24 giờ.
*   **Hiệu năng**: Khi tính cước, Worker đọc trực tiếp Snapshot từ Redis Cache thay vì join hàng chục bảng quan hệ PostgreSQL, giảm I/O database và tối ưu hóa thời gian xử lý.

### 2. Kafka Consumer & Điều Phối Spring Batch
*   **Kafka Consumer**:
    *   Lắng nghe các tác vụ tính cước gửi sang Kafka `billing-execution-topic` (chứa `BillingTaskDto`).
    *   Tiêu thụ tác vụ bất đồng bộ, sử dụng luồng ảo (Virtual Threads) để xử lý song song và đạt hiệu năng tối đa.
*   **Spring Batch Orchestrator**:
    *   Sử dụng Spring Batch để quản lý chu kỳ chốt cước theo Sổ (`Book_ID`).
    *   Thiết lập cấu hình `chunkSize` phù hợp để cân bằng giữa dung lượng bộ nhớ JVM và thông lượng ghi (throughput).
    *   Cấu hình cơ chế Skip/Retry chính xác cho Spring Batch để khi một khách hàng bị lỗi tính cước, Job không bị dừng đột ngột mà ghi nhận lỗi chi tiết vào nhật ký lỗi rồi tiếp tục xử lý các khách hàng khác.

### 3. Bulk Write & Transactional Outbox (Nhất Quán & Hiệu Năng)
*   **Transactional Outbox**:
    *   Sau khi `rating-engine` hoàn thành tính toán cước trên RAM, Worker thực hiện ghi nhận hóa đơn mới (`hoa_don`) và sự kiện gửi đi (`su_kien_outbox`) theo mô hình **Bulk Write** trong cùng một database transaction duy nhất.
    *   Việc này đảm bảo tính nhất quán ACID tuyệt đối (hoặc ghi nhận hóa đơn kèm outbox event, hoặc không ghi nhận gì cả nếu rollback).
*   **Debezium CDC Sync**:
    *   Debezium CDC Connector tự động phát hiện bản ghi outbox event mới được chèn vào bảng `su_kien_outbox` $\rightarrow$ Đẩy sự kiện hóa đơn chốt cước sang Kafka để CMIS hoặc hệ thống Hóa đơn điện tử tiêu thụ.

### 4. Không dùng giá trị mặc định khi thiếu dữ liệu trọng yếu
*   Khi thiết lập các tham số chạy cước tại `billing-worker` (ví dụ: áp dụng thuế VAT từ DB, gán thời gian biểu diễn từ chỉ số DTO sang thực thể `MeterUsage`), cấm tự ý gán các giá trị mặc định tĩnh làm che đậy lỗi cấu hình.
*   Nếu phát hiện cấu hình hoặc dữ liệu bắt buộc bị thiếu trong snapshot hay task đầu vào, Worker bắt buộc phải bắt exception, chuyển trạng thái tài khoản sang `FAILED`, ghi nhận thông tin chi tiết vào `nhat_ky_loi_tinh_toan`/`nhat_ky_chi_so` để phục vụ đối soát, và tiếp tục xử lý các tài khoản tiếp theo trong lô (Fail-safe).
