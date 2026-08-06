package com.evn.billing.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "su_kien_outbox")
@Getter
@Setter
public class OutboxEvent {

    @Id
    @GeneratedValue(generator = "UUID")
    @Column(name = "id_su_kien")
    private UUID idSuKien;

    @Column(name = "loai_doi_tuong", length = 50, nullable = false)
    private String loaiDoiTuong;

    @Column(name = "id_doi_tuong", length = 100, nullable = false)
    private String idDoiTuong;

    @Column(name = "loai_su_kien", length = 50, nullable = false)
    private String loaiSuKien;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "noi_dung", nullable = false)
    private String noiDung; // Stored as JSONB in DB

    @Column(name = "trang_thai", length = 20, nullable = false)
    private String trangThai = "PENDING"; // PENDING, SENT

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
}
