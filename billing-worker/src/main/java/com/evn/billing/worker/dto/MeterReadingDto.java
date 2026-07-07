package com.evn.billing.worker.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class MeterReadingDto {
    private String meterPointId;
    private LocalDateTime fromDate;
    private LocalDateTime toDate;
    private BigDecimal startIndex;
    private BigDecimal endIndex;
    private BigDecimal consumption;
    private Boolean isRollover;
    private BigDecimal maxRegisterSnapshot;
    private Integer subReadingSeq;
    private String recordType;

    public MeterReadingDto() {}

    public MeterReadingDto(String meterPointId, LocalDateTime fromDate, LocalDateTime toDate,
                           BigDecimal startIndex, BigDecimal endIndex, BigDecimal consumption,
                           Boolean isRollover, BigDecimal maxRegisterSnapshot, Integer subReadingSeq,
                           String recordType) {
        this.meterPointId = meterPointId;
        this.fromDate = fromDate;
        this.toDate = toDate;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.consumption = consumption;
        this.isRollover = isRollover;
        this.maxRegisterSnapshot = maxRegisterSnapshot;
        this.subReadingSeq = subReadingSeq;
        this.recordType = recordType;
    }

    public String getMeterPointId() { return meterPointId; }
    public void setMeterPointId(String meterPointId) { this.meterPointId = meterPointId; }

    public LocalDateTime getFromDate() { return fromDate; }
    public void setFromDate(LocalDateTime fromDate) { this.fromDate = fromDate; }

    public LocalDateTime getToDate() { return toDate; }
    public void setToDate(LocalDateTime toDate) { this.toDate = toDate; }

    public BigDecimal getStartIndex() { return startIndex; }
    public void setStartIndex(BigDecimal startIndex) { this.startIndex = startIndex; }

    public BigDecimal getEndIndex() { return endIndex; }
    public void setEndIndex(BigDecimal endIndex) { this.endIndex = endIndex; }

    public BigDecimal getConsumption() { return consumption; }
    public void setConsumption(BigDecimal consumption) { this.consumption = consumption; }

    public Boolean getIsRollover() { return isRollover; }
    public void setIsRollover(Boolean rollover) { isRollover = rollover; }

    public BigDecimal getMaxRegisterSnapshot() { return maxRegisterSnapshot; }
    public void setMaxRegisterSnapshot(BigDecimal maxRegisterSnapshot) { this.maxRegisterSnapshot = maxRegisterSnapshot; }

    public Integer getSubReadingSeq() { return subReadingSeq; }
    public void setSubReadingSeq(Integer subReadingSeq) { this.subReadingSeq = subReadingSeq; }

    public String getRecordType() { return recordType; }
    public void setRecordType(String recordType) { this.recordType = recordType; }
}
