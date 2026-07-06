# Thiết kế Thực thể & Mô hình Nghiệp vụ (Domain & Data Model Specification)

Tài liệu này đặc tả chi tiết các lớp Java Entity ánh xạ cơ sở dữ liệu, các lớp biểu diễn cấu trúc dữ liệu JSON động (Snapshot JSONB) và đặc tả logic nghiệp vụ của Core Rating Engine.

---

## 1. Bản đồ JPA Entities & Khởi tạo (Database Entity Models)

Sử dụng JPA/Hibernate định nghĩa ánh xạ các bảng vật lý.

### A. Lớp `Account` (Thông tin tĩnh Khách hàng)
```java
package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "account")
@Getter
@Setter
public class Account {
    @Id
    @Column(name = "account_id", length = 50)
    private String accountId;

    @Column(name = "book_id", length = 20, nullable = false)
    private String bookId;

    @Column(name = "customer_name", length = 100, nullable = false)
    private String customerName;

    @Column(name = "status", length = 20, nullable = false)
    private String status; // ACTIVE, SUSPENDED
}
```

### B. Lớp `MeterUsage` (Lịch sử sử dụng và chỉ số - Phân vùng)
```java
package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "meter_usage")
@IdClass(MeterUsageId.class) // Hỗ trợ composite key cho bảng phân vùng
@Getter
@Setter
public class MeterUsage {
    @Id
    @Column(name = "usage_id")
    private Long usageId;

    @Id
    @Column(name = "billing_cycle_month", length = 10)
    private String billingCycleMonth;

    @Column(name = "account_id", length = 50, nullable = false)
    private String accountId;

    @Column(name = "meter_point_id", length = 50, nullable = false)
    private String meterPointId;

    @Column(name = "from_date", nullable = false)
    private LocalDateTime fromDate;

    @Column(name = "to_date", nullable = false)
    private LocalDateTime toDate;

    @Column(name = "start_index", nullable = false, precision = 12, scale = 2)
    private BigDecimal startIndex;

    @Column(name = "end_index", nullable = false, precision = 12, scale = 2)
    private BigDecimal endIndex;

    @Column(name = "consumption", insertable = false, updatable = false, precision = 12, scale = 2)
    private BigDecimal consumption; // Dữ liệu tính sẵn tự động của DB (GENERATED ALWAYS)

    @Column(name = "status", length = 20, nullable = false)
    private String status; // VALIDATED, PENDING_MANUAL
}
```

---

## 2. Đặc tả Cấu trúc JSON đóng băng (Snapshot Config JSON Schema)

Trường `config_data` của bảng `BILLING_ACCOUNT_SNAPSHOT` được map với POJO Java `BillingConfigSnapshot` sử dụng Jackson:

```java
package com.evn.billing.common.dto;

import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class BillingConfigSnapshot {
    private String accountId;
    private int normsFactor;              // Định mức hộ dùng chung (ví dụ: 3 hộ)
    private MeterTopology meterTopology;  // Sơ đồ cây công tơ phụ tải
    private Map<String, TariffRules> tariffs; // Bản đồ các biểu giá sử dụng (Mã biểu giá -> Cấu hình bậc giá)
}
```

### A. Cấu trúc Cây công tơ (`MeterTopology`)
```java
@Data
public class MeterTopology {
    private List<MeterPointNode> rootPoints;
}

@Data
public class MeterPointNode {
    private String meterPointId;
    private CalculationType calculationType; // AGGREGATION (cộng dồn), NETTING (trừ phụ tải)
    private String tariffCode;                // Mã biểu giá áp dụng cho riêng điểm đo này (Có thể null nếu là công tơ netting phụ)
    private List<MeterPointNode> childPoints; // Công tơ phụ cấp dưới
}

public enum CalculationType {
    AGGREGATION,
    NETTING
}
```

### B. Quy tắc Biểu giá (`TariffRules`)
```java
@Data
public class TariffRules {
    private String tariffCode;
    private String type;                      // STEPPING (bậc thang), FLAT (đồng giá)...
    private List<TariffBlock> blocks;
}

@Data
public class TariffBlock {
    private int step;                         // Bậc số
    private double minKwh;                    // Sản lượng tối thiểu bậc
    private Double maxKwh;                    // Sản lượng tối đa bậc (null đối với bậc cuối cùng)
    private double unitPrice;                 // Đơn giá áp dụng
}
```

---

## 3. Đặc tả Logic xử lý Cây Công tơ (Netting Engine Logic)

Trước khi tính tiền điện, hệ thống duyệt đệ quy cây Topology để tính toán sản lượng điện thực tế cuối cùng.

```java
package com.evn.billing.engine;

import com.evn.billing.common.dto.MeterPointNode;
import com.evn.billing.common.dto.CalculationType;
import java.math.BigDecimal;
import java.util.Map;

public class TopologyCalculator {

    /**
     * Tính toán sản lượng thực nhận của một nút công tơ (Bao gồm cộng dồn con AGGREGATION và trừ bớt NETTING)
     * @param node Nút công tơ hiện tại
     * @param consumptions Map chứa sản lượng thô đo được của từng meter_point_id
     * @return Sản lượng Net cuối cùng của nhánh công tơ
     */
    public BigDecimal calculateNetConsumption(MeterPointNode node, Map<String, BigDecimal> consumptions) {
        BigDecimal nodeRaw = consumptions.getOrDefault(node.getMeterPointId(), BigDecimal.ZERO);
        BigDecimal netValue = nodeRaw;

        if (node.getChildPoints() != null) {
            for (MeterPointNode child : node.getChildPoints()) {
                BigDecimal childNet = calculateNetConsumption(child, consumptions);
                
                if (child.getCalculationType() == CalculationType.AGGREGATION) {
                    netValue = netValue.add(childNet);
                } else if (child.getCalculationType() == CalculationType.NETTING) {
                    netValue = netValue.subtract(childNet);
                }
            }
        }
        
        // Đảm bảo không âm (Trường hợp công tơ con netting đo nhiều hơn công tơ tổng do lệch mốc giờ)
        return netValue.compareTo(BigDecimal.ZERO) < 0 ? BigDecimal.ZERO : netValue;
    }
}
```

---

## 4. Logic Áp giá Bậc thang và Định mức Hộ (Rating Step Logic)

Thuật toán phân rã sản lượng điện tiêu thụ vào các bậc tương ứng đã được nhân định mức:

```java
package com.evn.billing.engine;

import com.evn.billing.common.dto.TariffBlock;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public class RatingStepEngine {

    @Data
    public static class StepResult {
        private int step;
        private BigDecimal kwhConsumed;
        private BigDecimal unitPrice;
        private BigDecimal amount;
    }

    public List<StepResult> calculateSteppingTariff(BigDecimal netConsumption, List<TariffBlock> standardBlocks, int normsFactor) {
        List<StepResult> results = new ArrayList<>();
        BigDecimal remainingKwh = netConsumption;
        BigDecimal norms = BigDecimal.valueOf(normsFactor);

        for (TariffBlock block : standardBlocks) {
            if (remainingKwh.compareTo(BigDecimal.ZERO) <= 0) {
                break;
            }

            // Mở rộng giới hạn bậc theo định mức hộ dùng chung
            BigDecimal blockMin = BigDecimal.valueOf(block.getMinKwh()).multiply(norms);
            BigDecimal blockMax = block.getMaxKwh() == null ? null : BigDecimal.valueOf(block.getMaxKwh()).multiply(norms);
            BigDecimal blockWidth = blockMax == null ? null : blockMax.subtract(blockMin);

            BigDecimal kwhInThisStep;
            if (blockWidth == null || remainingKwh.compareTo(blockWidth) < 0) {
                // Toàn bộ sản lượng còn lại thuộc bậc này
                kwhInThisStep = remainingKwh;
            } else {
                // Tràn bậc, lấy tối đa độ rộng của bậc
                kwhInThisStep = blockWidth;
            }

            StepResult result = new StepResult();
            result.setStep(block.getStep());
            result.setKwhConsumed(kwhInThisStep);
            result.setUnitPrice(BigDecimal.valueOf(block.getUnitPrice()));
            result.setAmount(kwhInThisStep.multiply(result.getUnitPrice()).setScale(2, RoundingMode.HALF_UP));
            
            results.add(result);
            remainingKwh = remainingKwh.subtract(kwhInThisStep);
        }

        return results;
    }
}
```
> [!TIP]
> Việc mở rộng khoảng bậc thang theo công thức `blockMin * norms` và `blockMax * norms` đảm bảo tính toán công bằng chính xác khi có nhiều hộ dân sử dụng chung một trạm hoặc một công tơ cơ sở.
