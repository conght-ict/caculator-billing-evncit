# Hướng dẫn Thiết lập và Kiểm thử Cục bộ (Local Development & Testing Guide for macOS)

Tài liệu này hướng dẫn lập trình viên cài đặt các thành phần hạ tầng cần thiết trên hệ điều hành macOS (MacBook) để chạy tích hợp và thực thi Unit Test cho hệ thống tính cước.

---

## 1. Yêu cầu Cài đặt ban đầu (Prerequisites)

### 1.1. Cài đặt Homebrew (Bộ quản lý gói của macOS)
Nếu chưa cài đặt, chạy lệnh sau trong Terminal:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 1.2. Cài đặt Java JDK 21 (Loom Virtual Threads support)
Sử dụng Homebrew để cài đặt OpenJDK 21:
```bash
brew install openjdk@21
```
*Lưu ý: Để thiết lập JAVA_HOME trỏ đúng vào JDK 21, thêm cấu hình sau vào file `~/.zshrc` hoặc `~/.bash_profile`:*
```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@21"
export PATH="$JAVA_HOME/bin:$PATH"
```

### 1.3. Cài đặt Apache Maven
Cài đặt Maven để đóng gói và kiểm thử mã nguồn Java:
```bash
brew install maven
```

### 1.4. Cài đặt Docker Desktop cho Mac
Tải và cài đặt trực tiếp từ trang chủ [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/) (Hỗ trợ tốt chip Apple Silicon M1/M2/M3 và Intel).

---

## 2. Khởi động Hạ tầng kiểm thử cục bộ (PostgreSQL, Redis, Kafka)

Hệ thống đã được trang bị tệp cấu hình docker compose sẵn sàng tại thư mục gốc của dự án. Để khởi chạy các dịch vụ:

1. Di chuyển vào thư mục gốc của dự án `Caculator-billing`.
2. Khởi chạy toàn bộ hạ tầng bằng lệnh:
```bash
docker compose up -d
```
Lệnh này sẽ tải và khởi động:
- **PostgreSQL** trên cổng `5432` (CSDL lưu trữ hóa đơn, outbox và cấu hình điểm đo).
- **Redis Cache** trên cổng `6379` (Bộ đệm lưu Snapshot).
- **Apache Kafka (KRaft mode)** trên cổng `9092` (Hàng đợi truyền tin chỉ số và tác vụ chốt cước).
- **Kafka UI** trên cổng `8089` (Giao diện web trực quan để giám sát topics và các message của Kafka, truy cập qua `http://localhost:8089`).

Để kiểm tra trạng thái các container:
```bash
docker compose ps
```

---

## 3. Chạy Unit Test cục bộ

Để chạy bộ kiểm thử Unit Test cho thuật toán tính cước (đặc biệt là Rating Engine):
```bash
mvn clean test
```
*Kết quả biên dịch và chạy test thành công (BUILD SUCCESS) xác nhận toàn bộ lô-gíc tính toán hoạt động chính xác trên RAM.*

---

## 4. Chạy Tích hợp các Microservices

Khi hạ tầng Docker đã online, khởi chạy các module theo thứ tự sau (mở mỗi lệnh trên một cửa sổ Terminal mới):

### Bước 1: Khởi chạy `snapshot-generator` (Cổng 8082)
Dịch vụ tạo và nạp Snapshot cấu hình. Khi khởi chạy lần đầu trên CSDL trống, module sẽ tự động sinh dữ liệu mồi (Seed) về điểm đo, quan hệ tổng phụ và biểu giá bậc thang trong CSDL.
```bash
cd snapshot-generator
mvn spring-boot:run
```

### Bước 2: Khởi chạy `mediation-service` (Cổng 8080)
Dịch vụ đón nhận chỉ số và tiếp nhận API CMIS.
```bash
cd mediation-service
mvn spring-boot:run
```

### Bước 3: Khởi chạy `billing-worker` (Cổng 8081)
Dịch vụ xử lý luồng ảo tính cước.
```bash
cd billing-worker
mvn spring-boot:run
```

### Bước 4: Khởi chạy `batch-orchestrator` (Cổng 8083)
Dịch vụ điều phối lô chốt sổ.
```bash
cd batch-orchestrator
mvn spring-boot:run
```

---

## 5. Các Lệnh gọi Thử nghiệm Tích hợp (API testing)

### Lệnh 1: Trigger đóng băng Snapshot
Tạo Snapshot chốt cước cho Sổ `SO_01` tháng `2026_06`:
```bash
curl -X POST "http://localhost:8082/api/v1/snapshots/generate?bookId=SO_01&month=2026_06"
```

### Lệnh 2: Kiểm tra điều kiện chốt cước của Sổ
```bash
curl -X GET "http://localhost:8083/api/v1/batch/validate?bookId=SO_01&month=2026_06"
```
*Trả về: `READY_FOR_BILLING` nếu tất cả chỉ số công tơ đã được nạp và kiểm duyệt hoàn chỉnh.*

### Lệnh 3: Kích hoạt chạy Batch chốt cước hàng loạt
```bash
curl -X POST "http://localhost:8083/api/v1/batch/run?bookId=SO_01&month=2026_06"
```
*Hệ thống Spring Batch sẽ quét danh sách khách hàng của Sổ SO_01, phân trang đẩy việc vào Kafka và Workers sẽ tự động tính toán ghi nhận hóa đơn ngầm.*

### Lệnh 4: Gọi tính cước On-demand khẩn cấp (Fallback)
Gửi yêu cầu tính cước khẩn cấp cho khách hàng `KH001` (chạy đồng bộ trên Virtual Threads):
```bash
curl -X POST http://localhost:8081/api/v1/billing/calculate-immediate \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "KH001",
    "billingCycleMonth": "2026_06",
    "calculationVersion": 1
  }'
```
