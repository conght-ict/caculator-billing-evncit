# Tài liệu Hướng dẫn Trực quan & Mô phỏng Luồng Tính Cước EVN (Cập Nhật Thực Tế)

Chúng tôi đã cập nhật sơ đồ tương tác nâng cao dưới dạng trang web HTML tích hợp để phản ánh đúng cấu hình và luồng truyền nhận tin qua hàng đợi Apache Kafka trong hệ thống phân tán thực tế.

## 1. Cách Mở và Sử Dụng Trang Web
Bạn có thể mở trực tiếp tệp tin bằng trình duyệt web của bạn thông qua đường dẫn sau:
👉 [docs/system_flow_diagram_light.html](file:///e:/caculator-billing-evncit/docs/system_flow_diagram_light.html)

---

## 2. Giao Diện Bản Đồ Luồng Xử Lý (Interactive Diagram Tab)
Bản đồ SVG tương tác hiện được chia làm **3 sơ đồ con** chuyên biệt giúp làm rõ từ quy trình nghiệp vụ tổng quan đến sơ đồ kỹ thuật chi tiết:

1. **Quy Trình Tự Động (Happy Path)**:
   * *Mô tả*: Sơ đồ chốt cước tự động cuốn chiếu lý tưởng cho các khách hàng "sạch" (không lỗi). Đi từ Lập lịch ghi chỉ số ở CMIS $\rightarrow$ Billing lưu lịch $\rightarrow$ Job quét lấy đo xa $\rightarrow$ Kiểm tra Ready (YES) $\rightarrow$ Tính cước trên RAM $\rightarrow$ Đánh giá Bất thường (NO) $\rightarrow$ Lưu hóa đơn tự động và đồng bộ ngược về CMIS.
2. **Luồng Kiểm Soát Ngoại Lệ (Cổng 1, 2, 3)**:
   * *Mô tả*: Sơ đồ rẽ nhánh chi tiết khi phát hiện ngoại lệ lỗi, mô tả cách CMIS kiểm soát các đối tượng lỗi độc lập không ảnh hưởng đến đối tượng sạch:
     * **Cổng 1**: Giữ các khách hàng lỗi đo xa/lỗi chỉ số (Ready = NO) để hiệu chỉnh tại CMIS và đẩy lại Billing tính toán.
     * **Cổng 2**: Giữ các khách hàng có hóa đơn bất thường (Sản lượng vọt x2 hoặc tiền điện > 1.000.000đ) để phê duyệt tính cước hoặc hủy tính cước trả về Cổng 1.
     * **Cổng 3**: Ký số lập hóa đơn điện tử hàng loạt và khóa trạng thái (LOCKED) cho các khách hàng sạch đã thành công.
3. **Luồng Xử Lý Chi Tiết (Technical Architecture)**:
   * *Mô tả*: Sơ đồ kiến trúc kỹ thuật chi tiết, mô tả cách các Microservices truyền nhận dữ liệu qua hàng đợi Kafka Broker, sử dụng Redis Cache để nạp cấu hình hợp đồng và sử dụng Virtual Threads (Java 21) để tính toán áp giá bậc thang song song theo khách hàng.

*Khi click vào từng cấu phần (ví dụ: CMIS Portal, Kafka Broker, Mediation, Worker, Redis, Postgres), bảng thông tin bên phải sẽ tự động hiển thị mô tả nghiệp vụ chi tiết kèm đoạn mã Java thực tế.*

---

## 3. Bộ Giả Lập Tác Nghiệp CMIS & Kafka Console (Simulator Tab)
Tab này tích hợp giả lập trực quan 2 phần:
* **Tác nghiệp CMIS**:
  * Chức năng lập lịch ghi chỉ số cho trạm.
  * Bảng quản lý hóa đơn trạm theo thời gian thực (hiển thị số lượng KH đo xa lỗi, chỉ số lỗi, bất thường, tính thành công). Người dùng có thể click trực tiếp vào số lỗi để sang giao diện xử lý chi tiết (hiệu chỉnh chỉ số lỗi Cổng 1, duyệt bất thường Cổng 2).
* **Kafka Console**: Hiển thị luồng thông điệp JSON thực tế được bắn qua lại giữa CMIS và Billing System tương ứng với mỗi thao tác tác nghiệp (Lập lịch, hiệu chỉnh chỉ số, duyệt cước, hủy cước).

---

## 4. Cấu Trúc Dữ Liệu (Database Tab)
* Hiển thị đặc tả cấu trúc bảng dữ liệu vật lý (Postgres DB) và cấu trúc khóa/snapshot trên Redis Cache hỗ trợ tính toán áp giá của Billing System.
