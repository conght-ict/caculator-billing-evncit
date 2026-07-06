# Đặc tả Công nghệ & Cấu hình Chi tiết (Technology Stack Specification)

Tài liệu này đặc tả chi tiết phiên bản thư viện, các tham số cấu hình hệ thống tối ưu cho cơ sở hạ tầng đã cài đặt (TiDB/PostgreSQL, Kafka Cluster, Redis Cluster, K8s).

---

## 1. Phiên bản Thư viện & Công nghệ (Dependency Matrix)

Hệ thống được phát triển dựa trên **Java 21 (LTS)** và **Spring Boot 3.3.x**.

| Thành phần | Thư viện & Phiên bản | Vai trò kỹ thuật |
| :--- | :--- | :--- |
| **JDK** | Eclipse Temurin OpenJDK 21 | Runtime environment, kích hoạt Project Loom (Virtual Threads). |
| **Framework** | Spring Boot 3.3.x | Bộ khung chính quản lý dependency Injection, Auto-configuration. |
| **Xử lý Batch** | Spring Batch 5.1.x | Định nghĩa luồng Job, Step, Chunk reader/writer phân tán. |
| **Kafka Client** | Spring Kafka (Kafka Client 3.7.x) | Kết nối, đẩy và nhận thông điệp tính cước. |
| **Redis Client** | Spring Data Redis (Lettuce Client 6.3.x) | Đọc ghi cache Snapshot đồng thời tốc độ cao. |
| **ORM / JDBC** | Spring Data JPA / Hibernate 6.5.x | Giao tiếp DB, ghi hóa đơn và outbox event. |
| **HikariCP** | HikariCP 5.1.0 | Quản lý kết nối DB (Connection Pool). |
| **DB Driver** | PostgreSQL JDBC Driver 42.7.x | Giao tiếp với TiDB Cluster hoặc PostgreSQL Citus. |
| **Giám sát** | Micrometer, Prometheus Registry | Thu thập metrics hiệu năng hệ thống. |
| **Tracing** | OpenTelemetry Bridge, Zipkin/OTLP | Ghi vết luồng đi của transaction từ chỉ số đến hóa đơn. |

---

## 2. Cấu hình Luồng Ảo (Project Loom - Virtual Threads)

Từ Java 21, luồng ảo (Virtual Threads) giúp tối ưu hóa hiệu năng cho các ứng dụng thực hiện nhiều tác vụ I/O blocking.

### A. Cấu hình Spring Boot
Kích hoạt luồng ảo cho các kết nối HTTP (Tomcat) và điều phối bất đồng bộ:
```yaml
spring:
  threads:
    virtual:
      enabled: true
```

### B. Cấu hình Spring Batch Task Executor
Sử dụng `SimpleAsyncTaskExecutor` đã cấu hình chạy trên Virtual Threads cho luồng Batch song song hoặc điều phối Worker:
```java
package com.evn.billing.batch.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

@Configuration
public class BatchExecutorConfig {

    @Bean
    public TaskExecutor virtualThreadTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch-virtual-");
        executor.setVirtualThreads(true); // Kích hoạt luồng ảo Java 21
        return executor;
    }
}
```

---

## 3. Cấu hình Tối ưu Connection Pool (HikariCP cho TiDB/PostgreSQL)

TiDB và Citus là các hệ CSDL phân tán hỗ trợ lượng lớn kết nối đồng thời. Để tối ưu tốc độ BULK ghi của Worker:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}?rewriteBatchedInserts=true&prepareThreshold=0
    username: ${DB_USER}
    password: ${DB_PASS}
    driver-class-name: org.postgresql.Driver
    hikari:
      pool-name: BillingHikariCP
      maximum-pool-size: 50             # Số lượng kết nối tối đa cho mỗi Pod Worker
      minimum-idle: 10                  # Giữ tối thiểu 10 kết nối nhàn rỗi
      idle-timeout: 30000               # 30 giây giải phóng kết nối nhàn rỗi
      max-lifetime: 1800000             # 30 phút đóng kết nối vật lý để refresh kết nối phân tán
      connection-timeout: 20000         # 20 giây timeout chờ kết nối rảnh
      auto-commit: false                # Tắt auto-commit để phục vụ Bulk commit theo lô (Chunk)
```

> [!NOTE]
> `rewriteBatchedInserts=true` là tham số bắt buộc của PostgreSQL Driver để gộp các câu lệnh `INSERT` riêng lẻ thành câu lệnh Insert theo lô (Multi-values Insert), giúp tăng tốc độ ghi DB lên 5 - 10 lần.

---

## 4. Cấu hình Tối ưu Apache Kafka (Spring Kafka)

### A. Cấu hình Kafka Consumer (Worker Node)
Để triệt tiêu hiện tượng **Kafka Rebalance** khi K8s scale Worker và bảo toàn thứ tự tính toán phụ tải theo `Account_ID`:

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_SERVERS}
    consumer:
      group-id: billing-worker-group
      # Gán group.instance.id tĩnh dựa trên Pod Name (Env Kubernetes)
      properties:
        group.instance.id: worker-pod-${HOSTNAME}
        session.timeout.ms: 30000             # Cho phép Pod tạm ngưng 30s trước khi rebalance
        max.poll.interval.ms: 300000          # 5 phút tối đa để xử lý xong một Chunk 1,000 Accounts
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties.spring.json.trusted.packages: "com.evn.billing.common.dto"
      auto-offset-reset: earliest
      enable-auto-commit: false               # Commit offset thủ công sau khi hoàn tất Bulk Write vào DB
```

### B. Cấu hình Kafka Producer (Batch Master Node)
Đảm bảo thông điệp tính cước không bao giờ bị mất hoặc bị ghi trùng lặp trên Broker:

```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all                               # Đợi toàn bộ Broker Replicas xác nhận
      properties:
        enable.idempotence: true              # Bật Idempotent Producer chống trùng lặp dữ liệu trên Kafka
        retries: 2147483647                   # Retry vô hạn đối với lỗi truyền dẫn mạng
        max.in.flight.requests.per.connection: 1 # Đảm bảo thứ tự tuyệt đối trên mỗi Partition
```

---

## 5. Cấu hình Redis Cache Layer (Lettuce Pool)

Sử dụng Redis Cluster để làm bộ đệm Snapshot tĩnh. Cần cấu hình Connection Pool cho Redis Client (Lettuce) để xử lý lượng truy cập đồng thời lớn:

```yaml
spring:
  data:
    redis:
      cluster:
        nodes: ${REDIS_NODES}                 # Danh sách các node trong Redis Cluster (IP:Port)
      lettuce:
        pool:
          max-active: 100                     # Tối đa 100 kết nối Redis hoạt động đồng thời mỗi Pod
          max-idle: 20                        # Tối đa 20 kết nối nhàn rỗi
          min-idle: 5
          max-wait: 2000ms                    # Thời gian tối đa chờ kết nối rảnh trong pool
      repositories:
        enabled: false                        # Tắt Redis Repositories mặc định để tự quản lý qua RedisTemplate
```

Cấu hình `RedisTemplate` sử dụng Jackson để serialize/deserialize Snapshot JSON tốc độ cao:
```java
package com.evn.billing.cache.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory, ObjectMapper objectMapper) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        Jackson2JsonRedisSerializer<Object> serializer = new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);
        
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(serializer);
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(serializer);
        
        return template;
    }
}
```

---

## 6. Tracing & Metrics (Observability)

Sử dụng Micrometer Tracing kết hợp OpenTelemetry gửi dữ liệu Trace về Grafana Tempo qua OTLP gRPC endpoint:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus
  otlp:
    tracing:
      endpoint: http://otel-collector.monitoring.svc.cluster.local:4317
  tracing:
    sampling:
      probability: 0.1                         # Thu thập 10% lượng Transaction thực tế
```
Khi chạy, mỗi tin nhắn Kafka sẽ được inject thêm header `traceparent` (W3C standard), đảm bảo **Trace ID** được truyền dẫn xuyên suốt từ Batch Master, qua Kafka Topic, đến Worker Node và lưu vết trong Database.
