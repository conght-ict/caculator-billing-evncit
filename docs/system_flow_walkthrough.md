# Tài liệu Hướng dẫn Trực quan & Mô phỏng Luồng Tính Cước EVN (Cập Nhật Thực Tế)

Chúng tôi đã cập nhật sơ đồ tương tác nâng cao dưới dạng trang web HTML tích hợp để phản ánh đúng cấu hình và luồng truyền nhận tin qua hàng đợi Apache Kafka trong hệ thống phân tán thực tế.

## 1. Cách Mở và Sử Dụng Trang Web
Bạn có thể mở trực tiếp tệp tin bằng trình duyệt web của bạn thông qua đường dẫn sau:
👉 [docs/system_flow_diagram.html](file:///e:/caculator-billing-evncit/docs/system_flow_diagram.html)

---

## 2. Giao Diện Bản Đồ Luồng Xử Lý (Interactive Diagram Tab)
Bản đồ SVG tương tác thể hiện chi tiết cách dữ liệu di chuyển từ hệ thống nguồn qua các hàng đợi Kafka, Redis Cache, và database Postgres cho 4 luồng nghiệp vụ cốt lõi:

1. **Luồng 1**: Tính cước cuốn chiếu tự động (Kafka Ingest)
   * *Mô tả*: CMIS đẩy chỉ số đo thô qua Kafka Topic `meter-readings-input`. `Mediation Service` lắng nghe qua `CmisIngestionListener`, lọc trùng, cập nhật lô vào DB để lưu vết lịch sử. Đồng thời, nó đối soát tính đầy đủ của sơ đồ topology, đóng gói trực tiếp danh sách chỉ số đo đếm thu thập được vào trong bản tin Task và gửi qua topic `billing-execution-topic`. Nhờ đó, `Billing Worker` nhận được Task là có sẵn chỉ số để tính toán ngay mà không cần tốn chi phí SELECT đọc lại DB Postgres.
2. **Luồng 2**: Chốt sổ hàng loạt (Spring Batch kích hoạt qua Kafka)
   * *Mô tả*: CMIS gửi yêu cầu chạy chốt sổ cho Mã Sổ qua Kafka Topic `cmis-batch-requests`. `Batch Orchestrator` lắng nghe, kích hoạt Job Spring Batch để làm nóng (Warm-up) Redis Cache, đọc phân trang chỉ số của các Hộ và nạp đồng loạt hàng nghìn Task tính cước đã chứa sẵn chỉ số chi tiết vào Kafka topic `billing-execution-topic` cho các Worker xử lý song song cực nhanh mà hoàn toàn không phát sinh I/O Read DB.
3. **Luồng 3**: On-Demand Fallback (Đồng Bộ REST)
   * *Mô tả*: Kịch bản khẩn cấp khi người dùng sửa chỉ số trên Exception Portal.
4. **Luồng 4**: Đồng Bộ Dữ Liệu Tĩnh (CMIS Master Data Sync)
   * *Mô tả*: CMIS gửi các thay đổi (biểu giá, hợp đồng, cây công tơ) qua Kafka Topic `cmis-masterdata-sync`. Hệ thống thu nhận dữ liệu mới, cập nhật bảng tĩnh trong database và tự động giải phóng (evict) Snapshot cũ trên Redis Cache để bảo đảm kết quả tính cước luôn mới nhất.

Khi click vào từng cấu phần (ví dụ: *Mediation*, *Worker*, *Redis*, *Postgres*), bảng thông tin bên phải sẽ tự động hiển thị mô tả nghiệp vụ kèm đoạn mã Java thực tế của cấu phần đó.

---

## 3. Bộ Giả Lập Tính Toán Nghiệp Vụ EVN (Calculation Simulator Tab)
Tab này cho phép bạn mô phỏng thuật toán tính cước điện bậc thang và cây công tơ netting:
* **Tính toán đệ quy**: Trừ trực tiếp sản lượng Công tơ phụ ra khỏi Công tơ tổng để ra sản lượng điện thương phẩm thực tế.
* **Mở rộng định mức bậc**: Tự động nhân tỷ lệ giới hạn các bậc thang cước dựa trên số hộ dùng chung (`norms_factor`) và số ngày thực tế sử dụng trong kỳ.
* **Biểu đồ trực quan**: Vẽ biểu đồ phân bổ sản lượng điện tiêu thụ rơi vào từng bậc cước sinh hoạt của EVN.

---

## 4. Bảng Giám Sát Thời Gian Thực (Monitoring Dashboard Tab)
* Mô phỏng giao diện giám sát hiệu năng TPS (số hóa đơn xử lý/giây), số lượng luồng ảo Virtual Threads đang sinh ra và độ trễ Kafka Lag.
* Hiển thị log dòng vết tính toán chi tiết của Worker chạy ngầm theo thời gian thực (bao gồm các bước Kafka Ingestion và CMIS Master Data Sync).
