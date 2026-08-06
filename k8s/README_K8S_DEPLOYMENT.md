# 📖 HƯỚNG DẪN TRIỂN KHAI TOÀN BỘ HỆ THỐNG EVN BILLING LÊN KUBERNETES (FROM SCRATCH)

Tài liệu này là **Cẩm nang tổng hợp đầy đủ từng bước từ A - Z** giúp bạn tự tay thiết lập môi trường Kubernetes, đóng gói mã nguồn và triển khai phân hệ Core Tính Hóa Đơn EVN CMIS tại máy tính cá nhân.

---

## 📋 MỤC LỤC
1. [Bước 1: Bật Cụm Kubernetes Trên Máy Tính](#bước-1-bật-cụm-kubernetes-trên-máy-tính)
2. [Bước 2: Khởi Tạo Hạ Tầng Phụ Trợ (Kafka, Redis, PostgreSQL)](#bước-2-khởi-tạo-hạ-tầng-phụ-trợ-kafka-redis-postgresql)
3. [Bước 3: Biên Dịch Code & Deploy Lên Kubernetes](#bước-3-biên-dịch-code--deploy-lên-kubernetes)
4. [Bước 4: Quy Trình CI/CD Tự Động Khi Sửa Code](#bước-4-quy-trình-cicd-tự-động-khi-sửa-code)
5. [Bước 5: Lệnh Kiểm Tra & Dọn Dẹp Dịch Vụ Cũ](#bước-5-lệnh-kiểm-tra--dọn-dẹp-dịch-vụ-cũ)

---

## 🛠️ BƯỚC 1: BẬT CỤM KUBERNETES TRÊN MÁY TÍNH

Nếu máy bạn chưa bật Kubernetes:
1. Mở giao diện ứng dụng **Docker Desktop**.
2. Nhấp vào biểu tượng **Bánh răng Cài đặt (Settings)** ở góc trên bên phải.
3. Chọn tab **Kubernetes** ở menu bên trái.
4. Tích chọn ô **Enable Kubernetes**.
5. Nhấp nút **Apply & restart** và chờ 3 - 5 phút cho đến khi Docker báo biểu tượng Kubernetes màu xanh lá (`Kubernetes is running`).

---

## 🗄️ BƯỚC 2: KHỞI TẠO HẠ TẦNG PHỤ TRỢ (KAFKA, REDIS, POSTGRESQL)

Mở Terminal (PowerShell / CMD) tại thư mục gốc dự án `e:\caculator-billing-evncit`:

```powershell
# Chạy cụm 3 Kafka Brokers, 6 Redis Cluster Nodes, 2 PostgreSQL Primary/Standby & Monitoring Dashboards
docker compose up -d
```

*Kiểm tra:*
*   👉 Kafka UI: `http://localhost:8989`
*   👉 Debezium UI: `http://localhost:8990`
*   👉 Grafana Dashboard: `http://localhost:3003`

---

## 🚀 BƯỚC 3: BIÊN DỊCH CODE & DEPLOY LÊN KUBERNETES

### Cách 1: Sử Dụng Lệnh 1-Click Tự Động (Khuyên Dùng)

Mở PowerShell dưới quyền người dùng hiện tại, mở khóa quyền chạy script (1 giây):
```powershell
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process -Force
```

Chạy tập lệnh 1-Click:
```powershell
.\deploy-local.ps1
```

> **Script sẽ tự động:**
> 1. Biên dịch file `.jar` bằng Maven.
> 2. Đóng gói Docker Images local cho các module.
> 3. Nạp cấu hình vào cụm Kubernetes `evn-billing`.

---

### Cách 2: Thực Hiện Thủ Công Từng Câu Lệnh (Nếu muốn tự gõ)

```powershell
# 1. Tạo Namespace & ConfigMap trên K8s
kubectl apply -f k8s/00-namespace-config.yaml

# 2. Biên dịch mã nguồn Java
mvn clean package -DskipTests

# 3. Build Docker Image cho từng Service
docker build -t billing-worker:latest ./billing-worker
docker build -t batch-orchestrator:latest ./batch-orchestrator
docker build -t mediation-service:latest ./mediation-service

# 4. Deploy các Deployment lên Kubernetes
kubectl apply -f k8s/01-billing-worker-deployment.yaml
kubectl apply -f k8s/02-batch-orchestrator-deployment.yaml
kubectl apply -f k8s/03-mediation-service-deployment.yaml
kubectl apply -f k8s/04-debezium-cdc-deployment.yaml
```

---

## 🔄 BƯỚC 4: QUY TRÌNH CI/CD TỰ ĐỘNG KHl SỬA CODE

Mỗi khi bạn chỉnh sửa bất kỳ dòng code nào trong dự án:

1. Chỉ cần gõ duy nhất 1 câu lệnh:
   ```powershell
   .\deploy-local.ps1
   ```
2. **Cơ chế thông minh của script:**
   * Script sẽ tự động dùng Git để quét xem bạn vừa sửa module nào (`billing-worker`, `batch-orchestrator` hay `mediation-service`).
   * Chỉ biên dịch Maven và Re-deploy duy nhất module bị thay đổi code.
   * Tốc độ nâng cấp Pods giảm từ vài phút xuống còn **vài giây**!

---

## 🧹 BƯỚC 5: LỆNH KIỂM TRA & DỌN DẸP DỊCH VỤ CŨ

### 1. Kiểm tra danh sách Pods đang chạy của hệ thống EVN Billing:
```powershell
kubectl get pods -n evn-billing -o wide
```

### 2. Xem Nhật ký (Logs) tính toán cước thực tế của Worker:
```powershell
kubectl logs -f -l app=billing-worker -n evn-billing --tail=100
```

### 3. Xóa bớt các Pods dư thừa từ dự án cũ ở namespace default (nếu muốn dọn đẹp đĩa):
```powershell
kubectl delete deployment gateway-service order-service inventory-service -n default
```
