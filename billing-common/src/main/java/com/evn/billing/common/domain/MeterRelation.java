package com.evn.billing.common.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDate;

@Entity
@Table(name = "meter_relation")
@Data
public class MeterRelation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "relation_id")
    private Long relationId;

    @Column(name = "parent_id", length = 50, nullable = false)
    private String parentId;

    @Column(name = "child_id", length = 50, nullable = false)
    private String childId;

    @Column(name = "relation_type", length = 20, nullable = false)
    private String relationType; // AGGREGATION, NETTING

    @Column(name = "effective_from", nullable = false)
    private LocalDate effectiveFrom;

    @Column(name = "effective_to")
    private LocalDate effectiveTo;
}
