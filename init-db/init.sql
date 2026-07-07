-- =========================================================================
-- DATABASE INITIALIZATION SCRIPT v3.0
-- EVN National Billing System — Optimized Schema with Consolidated Run State
-- =========================================================================
-- Constraint Matrix applied in this file:
--   [I.1]  Append-Only: record_type ORIGINAL/CORRECTION, no UPDATE on raw readings
--   [I.2]  Monotonically Increasing: CHECK(to_date > from_date), is_rollover, max_register_value
--   [I.3]  Deduplication: UNIQUE(meter_point_id, from_date, to_date) for ORIGINAL records
--   [II.1] Snapshot Isolation: NO Foreign Keys to Master Data tables
--   [II.2] Self-Containment: JSONB must contain 6 required fields; Malform -> DLQ
--   [III.1] Locality Sharding: Kafka Partition Key = Account_ID (documented in code layer)
--   [III.2] Backpressure: max.poll.records=50, Pause/Resume at buffer>500 (app config)
--   [IV.1] Idempotency: UNIQUE(idempotency_key) + UPSERT ON CONFLICT (documented)
--   [IV.2] Self-Explainability: billing_manifest JSONB NOT NULL (includes rounding_mode)
-- =========================================================================

-- =========================================================================
-- GROUP 0: MASTER DATA (Static, ACID, Low mutation rate)
-- =========================================================================

-- 0a. Meter Model Catalog [Q1: Register Rollover threshold per hardware model]
CREATE TABLE meter_model (
    model_code          VARCHAR(50) PRIMARY KEY,
    manufacturer        VARCHAR(100) NOT NULL,
    model_name          VARCHAR(100) NOT NULL,
    max_register_value  DECIMAL(14,2) NOT NULL DEFAULT 99999.9,
    -- [I.2] Physical rollover threshold (e.g., 99999.9 kWh for 5-digit meters)
    display_digits      INT NOT NULL DEFAULT 5,
    meter_type          VARCHAR(20) NOT NULL DEFAULT 'MECHANICAL',
    -- MECHANICAL | ELECTRONIC | SMART_AMI
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 1. Account (Master, Shard by account_id)
CREATE TABLE account (
    account_id          VARCHAR(50) PRIMARY KEY,
    book_id             VARCHAR(50) NOT NULL,
    customer_name       VARCHAR(100) NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    norms_factor        INT NOT NULL DEFAULT 1,
    address             TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_account_book ON account(book_id);

-- 1b. Book Billing Schedule & Run Progress (Consolidated Schedule & Execution State) [Q2/Q3]
CREATE TABLE book_billing_schedule (
    book_id             VARCHAR(50) NOT NULL,
    billing_cycle_month VARCHAR(20) NOT NULL, -- Format: YYYY_MM (e.g., '2026_06')
    period              INT NOT NULL DEFAULT 1, -- Kỳ thứ mấy trong tháng (1, 2, 3...)
    from_date           DATE NOT NULL, -- Ngày bắt đầu kỳ cước
    to_date             DATE NOT NULL, -- Ngày chốt kỳ cước
    
    status              VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | CLOSED
    run_status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- PENDING | SNAPSHOT_GENERATING | PROCESSING | COMPLETED | FAILED
    total_accounts      INT DEFAULT 0,
    processed_accounts  INT DEFAULT 0,
    success_accounts    INT DEFAULT 0,
    failed_accounts     INT DEFAULT 0,
    triggered_by        VARCHAR(20) DEFAULT 'CMIS',
    
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (book_id, billing_cycle_month, period),
    CONSTRAINT chk_book_schedule_dates CHECK (to_date >= from_date)
);

-- 2. Meter Point (Master, References meter_model for rollover spec)
CREATE TABLE meter_point (
    meter_point_id      VARCHAR(50) PRIMARY KEY,
    account_id          VARCHAR(50) NOT NULL REFERENCES account(account_id),
    model_code          VARCHAR(50) REFERENCES meter_model(model_code),
    tariff_code         VARCHAR(50) NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    meter_serial        VARCHAR(100),
    installed_date      DATE,
    decommission_date   DATE
);
CREATE INDEX idx_meter_point_account ON meter_point(account_id);

-- 3. Meter Relation (Topology DAG)
CREATE TABLE meter_relation (
    relation_id     BIGSERIAL PRIMARY KEY,
    parent_id       VARCHAR(50) NOT NULL REFERENCES meter_point(meter_point_id),
    child_id        VARCHAR(50) NOT NULL REFERENCES meter_point(meter_point_id),
    relation_type   VARCHAR(20) NOT NULL,
    -- NETTING | AGGREGATION
    effective_from  DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE,
    CONSTRAINT chk_different_meters CHECK (parent_id <> child_id)
);
CREATE INDEX idx_meter_relation_parent ON meter_relation(parent_id);

-- =========================================================================
-- GROUP Metadata: TARIFF RULES (Temporal Validity) [Q3]
-- =========================================================================

-- 4. Tariff with effective_date + expiry_date for GetTariffAt(target_date)
CREATE TABLE tariff (
    tariff_code     VARCHAR(50) PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    type            VARCHAR(20) NOT NULL,
    -- STEPPING | FLAT | TOU
    effective_date  DATE NOT NULL,
    expiry_date     DATE,
    -- NULL = currently active
    issued_by       VARCHAR(300),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_tariff_temporal ON tariff(type, effective_date, expiry_date);

-- 5. Tariff Detail (Price blocks)
CREATE TABLE tariff_detail (
    detail_id       BIGSERIAL PRIMARY KEY,
    tariff_code     VARCHAR(50) NOT NULL REFERENCES tariff(tariff_code) ON DELETE CASCADE,
    step            INT NOT NULL,
    min_kwh         DECIMAL(12,2) NOT NULL,
    max_kwh         DECIMAL(12,2),
    unit_price      DECIMAL(12,2) NOT NULL,
    tou_period      VARCHAR(20)
    -- PEAK | OFF_PEAK | NORMAL (for TOU type only)
);
CREATE INDEX idx_tariff_detail_code ON tariff_detail(tariff_code, step);

-- =========================================================================
-- GROUP I: USAGE / TELEMETRY DATA (Write-heavy, Append-Only, Partitioned)
-- =========================================================================

-- 6. Meter Usage (Partitioned by billing_cycle_month)
CREATE TABLE meter_usage (
    usage_id                BIGINT NOT NULL,
    sub_reading_seq         INT NOT NULL DEFAULT 1,
    -- [I.2 Edge Case] Meter replacement mid-period: seq=1 old meter, seq=2 new meter

    account_id              VARCHAR(50) NOT NULL,
    meter_point_id          VARCHAR(50) NOT NULL,
    billing_cycle_month     VARCHAR(20) NOT NULL,
    period                  INT NOT NULL DEFAULT 1,

    from_date               TIMESTAMP NOT NULL,
    to_date                 TIMESTAMP NOT NULL,
    CONSTRAINT chk_meter_usage_date_order CHECK (to_date > from_date),
    -- [I.2] Monotonically Increasing time constraint

    start_index             DECIMAL(14,2) NOT NULL,
    end_index               DECIMAL(14,2) NOT NULL,

    is_rollover             BOOLEAN NOT NULL DEFAULT FALSE,
    -- [I.2] TRUE when end_index < start_index due to hardware counter reset
    max_register_snapshot   DECIMAL(14,2),
    -- Snapshot of meter_model.max_register_value AT TIME OF READING

    raw_consumption         DECIMAL(14,2) NOT NULL,
    -- Pre-computed by Mediation, stored to avoid re-computation

    status                  VARCHAR(20) NOT NULL DEFAULT 'PENDING_MANUAL',
    -- PENDING_MANUAL | VALIDATED | TELEMETRY

    record_type             VARCHAR(20) NOT NULL DEFAULT 'ORIGINAL',
    -- [I.1] ORIGINAL: first reading | CORRECTION: operator override (append, not update)
    correction_of_usage_id  BIGINT,
    -- [I.1] Soft FK to corrected record (no physical FK for audit isolation)

    source                  VARCHAR(20) NOT NULL DEFAULT 'AMR',
    -- AMR | HANDHELD | MANUAL

    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (usage_id, sub_reading_seq, billing_cycle_month, period)
) PARTITION BY RANGE (billing_cycle_month);

-- [I.3] Deduplication: Prevent overlapping time-ranges for same meter point
CREATE UNIQUE INDEX uq_meter_usage_no_overlap
    ON meter_usage (meter_point_id, from_date, to_date);

-- Partitions
CREATE TABLE meter_usage_2026_06 PARTITION OF meter_usage FOR VALUES FROM ('2026_06') TO ('2026_07');
CREATE TABLE meter_usage_2026_07 PARTITION OF meter_usage FOR VALUES FROM ('2026_07') TO ('2026_08');
CREATE TABLE meter_usage_2026_08 PARTITION OF meter_usage FOR VALUES FROM ('2026_08') TO ('2026_09');

CREATE INDEX idx_meter_usage_lookup ON meter_usage(account_id, billing_cycle_month, status);
CREATE INDEX idx_meter_usage_point  ON meter_usage(meter_point_id, billing_cycle_month);

-- =========================================================================
-- GROUP II: SNAPSHOT DATA (Frozen, Read-Only, Self-Contained JSONB)
-- =========================================================================

-- 7. Billing Account Snapshot [II.1: No FK, II.2: Self-Contained JSONB]
CREATE TABLE billing_account_snapshot (
    snapshot_id             VARCHAR(200) PRIMARY KEY,
    -- Format: {account_id}_{billing_cycle_month}_p{period}_v{version}

    account_id              VARCHAR(50) NOT NULL,
    -- [II.1] NO REFERENCES: physically decoupled from Master Data tables
    book_id                 VARCHAR(50) NOT NULL,
    billing_cycle_month     VARCHAR(20) NOT NULL,
    period                  INT NOT NULL DEFAULT 1,
    calculation_version     INT NOT NULL DEFAULT 1,

    effective_sync_date     DATE NOT NULL,
    config_data             JSONB NOT NULL,
    -- [II.2] Self-Contained: MUST include all 6 required fields:
    --   1. account_id  2. book_id  3. norms_factor
    --   4. effective_sync_date  5. meter_topology  6. tariffs

    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_snapshot_composite
    ON billing_account_snapshot(account_id, billing_cycle_month, period, calculation_version);
CREATE INDEX idx_snapshot_book
    ON billing_account_snapshot(book_id, billing_cycle_month, period);
CREATE INDEX idx_snapshot_jsonb
    ON billing_account_snapshot USING GIN (config_data);

-- =========================================================================
-- GROUP IV: TRANSACTIONAL / OUTPUT DATA (Idempotent, Partitioned)
-- =========================================================================

-- 8. Bill Invoice [IV.1: Idempotency, IV.2: Self-Explainability]
CREATE TABLE bill_invoice (
    invoice_id              VARCHAR(100) NOT NULL,
    account_id              VARCHAR(50) NOT NULL,
    book_id                 VARCHAR(50) NOT NULL,
    billing_cycle_month     VARCHAR(20) NOT NULL,
    period                  INT NOT NULL DEFAULT 1,
    total_amount_before_tax DECIMAL(15,2) NOT NULL,
    tax_amount              DECIMAL(15,2) NOT NULL,
    total_amount_after_tax  DECIMAL(15,2) NOT NULL,

    idempotency_key         VARCHAR(200) NOT NULL,
    -- [IV.1] = account_id + '_' + billing_cycle_month + '_p' + period + '_v' + calculation_version
    -- Worker MUST use UPSERT: INSERT ... ON CONFLICT (idempotency_key) DO UPDATE

    billing_manifest        JSONB NOT NULL,
    -- [IV.2] NEVER NULL. Contains:
    --   topology_calculation (sub_readings, is_rollover, net_consumption)
    --   rating_breakdown (steps, amounts, norms_factor applied)
    --   tax_calculation (vat_rate, rounding_mode: HALF_UP, tax_amount_final)
    --   total_final_amount, calculation_engine_version, snapshot_applied

    proration_applied       BOOLEAN NOT NULL DEFAULT FALSE,
    snapshot_ref            VARCHAR(200),
    calculation_status      VARCHAR(20) NOT NULL DEFAULT 'FINAL',

    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (invoice_id, billing_cycle_month),
    CONSTRAINT uq_idempotency_invoice UNIQUE (idempotency_key)
) PARTITION BY RANGE (billing_cycle_month);

CREATE TABLE bill_invoice_2026_06 PARTITION OF bill_invoice FOR VALUES FROM ('2026_06') TO ('2026_07');
CREATE TABLE bill_invoice_2026_07 PARTITION OF bill_invoice FOR VALUES FROM ('2026_07') TO ('2026_08');
CREATE TABLE bill_invoice_2026_08 PARTITION OF bill_invoice FOR VALUES FROM ('2026_08') TO ('2026_09');

CREATE INDEX idx_invoice_account ON bill_invoice(account_id, billing_cycle_month);
CREATE INDEX idx_invoice_book    ON bill_invoice(book_id, billing_cycle_month);

-- =========================================================================
-- GROUP: INTEGRATION & AUDIT TABLES
-- =========================================================================

-- 9. Transactional Outbox (Written in SAME tx as bill_invoice)
CREATE TABLE outbox_event (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(50) NOT NULL,
    aggregate_id    VARCHAR(100) NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    payload         JSONB NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_outbox_pending ON outbox_event(status, created_at) WHERE status = 'PENDING';

-- 10. Account Billing Status (Partitioned per-account progress and log audit details)
CREATE TABLE account_billing_status (
    account_id              VARCHAR(50) NOT NULL,
    billing_cycle_month     VARCHAR(20) NOT NULL,
    book_id                 VARCHAR(50) NOT NULL,
    period                  INT NOT NULL DEFAULT 1,
    status                  VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- PENDING | PROCESSING | SUCCESS | FAILED | DLQ
    invoice_id              VARCHAR(100),
    error_message           TEXT,
    retry_count             INT DEFAULT 0,
    processing_time_ms      BIGINT,
    worker_node             VARCHAR(100),
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, billing_cycle_month, period)
) PARTITION BY LIST (billing_cycle_month);

CREATE TABLE account_billing_status_default PARTITION OF account_billing_status DEFAULT;

CREATE INDEX idx_acc_bill_status_book ON account_billing_status(book_id, billing_cycle_month, status);

-- 11. Detailed Billing Calculation Log (For detail audit tracking of input consumptions and output manifest)
CREATE TABLE billing_calculation_log (
    log_id                  UUID PRIMARY KEY,
    book_id                 VARCHAR(50) NOT NULL,
    account_id              VARCHAR(50) NOT NULL,
    billing_cycle_month     VARCHAR(20) NOT NULL,
    period                  INT NOT NULL DEFAULT 1,
    status                  VARCHAR(20) NOT NULL, -- SUCCESS | FAILED
    input_data              JSONB,
    output_data             JSONB,
    error_message           TEXT,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_calc_log_book ON billing_calculation_log(book_id, billing_cycle_month, period);
CREATE INDEX idx_calc_log_account ON billing_calculation_log(account_id, billing_cycle_month, period);

-- =========================================================================
-- SEED DATA
-- =========================================================================

-- Meter Models
INSERT INTO meter_model (model_code, manufacturer, model_name, max_register_value, display_digits, meter_type) VALUES
('MODEL_MECH_5D',   'Dien Co Thong Nhat', 'Cong to co 5 so',       99999.9,  5, 'MECHANICAL'),
('MODEL_MECH_6D',   'Dien Co Thong Nhat', 'Cong to co 6 so',       999999.9, 6, 'MECHANICAL'),
('MODEL_ELEC_5D',   'EDMI',               'Cong to dien tu 5 so',  99999.9,  5, 'ELECTRONIC'),
('MODEL_SMART_AMI', 'Hexing',             'Cong to AMI thong minh', 9999999.9, 7, 'SMART_AMI');

-- Tariffs (with Temporal Validity)
INSERT INTO tariff (tariff_code, name, type, effective_date, expiry_date, issued_by) VALUES
('TARIFF_SHBT_2023',   'Sinh hoat bac thang 2023',            'STEPPING', '2023-05-04', NULL, 'QD 648/QD-BCT 20/03/2023'),
('TARIFF_KDOANH_2023', 'Kinh doanh dong gia 2023',            'FLAT',     '2023-05-04', NULL, 'QD 648/QD-BCT 20/03/2023'),
('TARIFF_SX_2023',     'San xuat dong gia 2023',              'FLAT',     '2023-05-04', NULL, 'QD 648/QD-BCT 20/03/2023'),
('TARIFF_SX_BT',       'San xuat - Gio Binh thuong (TOU)',    'TOU',      '2023-05-04', NULL, 'QD 648/QD-BCT 20/03/2023'),
('TARIFF_SX_CD',       'San xuat - Gio Cao diem (TOU)',       'TOU',      '2023-05-04', NULL, 'QD 648/QD-BCT 20/03/2023'),
('TARIFF_SX_TD',       'San xuat - Gio Thap diem (TOU)',      'TOU',      '2023-05-04', NULL, 'QD 648/QD-BCT 20/03/2023');

-- Tariff Details
INSERT INTO tariff_detail (tariff_code, step, min_kwh, max_kwh, unit_price) VALUES
('TARIFF_SHBT_2023', 1,   0,  50, 1806.00),
('TARIFF_SHBT_2023', 2,  50, 100, 1866.00),
('TARIFF_SHBT_2023', 3, 100, 200, 2167.00),
('TARIFF_SHBT_2023', 4, 200, 300, 2729.00),
('TARIFF_SHBT_2023', 5, 300, 400, 3050.00),
('TARIFF_SHBT_2023', 6, 400,NULL, 3157.00),
('TARIFF_KDOANH_2023', 1, 0, NULL, 2500.00),
('TARIFF_SX_2023',     1, 0, NULL, 2000.00),
('TARIFF_SX_BT', 1, 0, NULL, 1800.00),
('TARIFF_SX_CD', 1, 0, NULL, 3200.00),
('TARIFF_SX_TD', 1, 0, NULL, 1100.00);

-- Book Billing Schedules
INSERT INTO book_billing_schedule (book_id, billing_cycle_month, period, from_date, to_date, status) VALUES
('SO_01', '2026_06', 1, '2026-06-01', '2026-06-10', 'CLOSED'),
('SO_01', '2026_06', 2, '2026-06-11', '2026-06-20', 'CLOSED'),
('SO_01', '2026_06', 3, '2026-06-21', '2026-06-30', 'ACTIVE'),
('SO_01', '2026_07', 1, '2026-07-01', '2026-07-31', 'ACTIVE');

-- Accounts
INSERT INTO account (account_id, book_id, customer_name, status, norms_factor) VALUES
('KH001', 'SO_01', 'Nguyen Van A (SHBT 1 ho)',          'ACTIVE', 1),
('KH002', 'SO_01', 'Tran Thi B (San xuat don gia)',     'ACTIVE', 1),
('KH003', 'SO_01', 'Cong ty C (Netting + norms=3)',     'ACTIVE', 3),
('KH005', 'SO_01', 'Nha may E (San xuat TOU 3 gia)',    'ACTIVE', 1),
('KH006', 'SO_01', 'Ho F (Test Register Rollover)',     'ACTIVE', 1);

-- Meter Points
INSERT INTO meter_point (meter_point_id, account_id, model_code, tariff_code, status, meter_serial) VALUES
('METER-01',      'KH001', 'MODEL_MECH_5D',   'TARIFF_SHBT_2023',   'ACTIVE', 'SN-11111'),
('METER-02',      'KH002', 'MODEL_ELEC_5D',   'TARIFF_SX_2023',     'ACTIVE', 'SN-22222'),
('METER-03-TONG', 'KH003', 'MODEL_SMART_AMI', 'TARIFF_SHBT_2023',   'ACTIVE', 'SN-33300'),
('METER-03-PHU',  'KH003', 'MODEL_ELEC_5D',   'TARIFF_KDOANH_2023', 'ACTIVE', 'SN-33301'),
('METER-05-BT',   'KH005', 'MODEL_ELEC_5D',   'TARIFF_SX_BT',       'ACTIVE', 'SN-55551'),
('METER-05-CD',   'KH005', 'MODEL_ELEC_5D',   'TARIFF_SX_CD',       'ACTIVE', 'SN-55552'),
('METER-05-TD',   'KH005', 'MODEL_ELEC_5D',   'TARIFF_SX_TD',       'ACTIVE', 'SN-55553'),
('METER-06',      'KH006', 'MODEL_MECH_5D',   'TARIFF_SHBT_2023',   'ACTIVE', 'SN-66666');

-- Meter Relations (Netting)
INSERT INTO meter_relation (parent_id, child_id, relation_type, effective_from) VALUES
('METER-03-TONG', 'METER-03-PHU', 'NETTING', '2020-01-01');

-- Meter Usage — 2026_06 (VALIDATED, ORIGINAL records)
INSERT INTO meter_usage (
    usage_id, sub_reading_seq, account_id, meter_point_id, billing_cycle_month, period,
    from_date, to_date, start_index, end_index,
    is_rollover, max_register_snapshot, raw_consumption,
    status, record_type, source
) VALUES
-- KH001: 250 kWh standard
(1, 1, 'KH001', 'METER-01', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 1000.00, 1250.00, FALSE, 99999.9, 250.00, 'VALIDATED', 'ORIGINAL', 'AMR'),

-- KH002: 10000 kWh industrial
(2, 1, 'KH002', 'METER-02', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 5000.00, 15000.00, FALSE, 99999.9, 10000.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD'),

-- KH003: Netting — TONG=500kWh, PHU=100kWh, Net=400kWh
(3, 1, 'KH003', 'METER-03-TONG', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 2000.00, 2500.00, FALSE, 9999999.9, 500.00, 'VALIDATED', 'ORIGINAL', 'AMR'),
(4, 1, 'KH003', 'METER-03-PHU', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 500.00, 600.00, FALSE, 99999.9, 100.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD'),

-- KH005: TOU 3-rate (BT=1000, CD=200, TD=500 kWh)
(8, 1, 'KH005', 'METER-05-BT', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 1000.00, 2000.00, FALSE, 99999.9, 1000.00, 'VALIDATED', 'ORIGINAL', 'AMR'),
(9, 1, 'KH005', 'METER-05-CD', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 500.00, 700.00, FALSE, 99999.9, 200.00, 'VALIDATED', 'ORIGINAL', 'AMR'),
(10, 1, 'KH005', 'METER-05-TD', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 300.00, 800.00, FALSE, 99999.9, 500.00, 'VALIDATED', 'ORIGINAL', 'AMR'),

-- KH006: REGISTER ROLLOVER [I.2 Edge Case]
(11, 1, 'KH006', 'METER-06', '2026_06', 3,
 '2026-06-01 00:00:00', '2026-06-30 23:59:59',
 99900.00, 100.00, TRUE, 99999.9, 199.90, 'VALIDATED', 'ORIGINAL', 'HANDHELD');

-- Meter Usage — 2026_07 (METER REPLACEMENT MID-PERIOD [I.2 Edge Case])
INSERT INTO meter_usage (
    usage_id, sub_reading_seq, account_id, meter_point_id, billing_cycle_month, period,
    from_date, to_date, start_index, end_index,
    is_rollover, max_register_snapshot, raw_consumption,
    status, record_type, source
) VALUES
-- seq=1: Old meter, 01/07 to 10/07 = 50 kWh
(12, 1, 'KH001', 'METER-01', '2026_07', 1,
 '2026-07-01 00:00:00', '2026-07-10 23:59:59',
 1050.00, 1100.00, FALSE, 99999.9, 50.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD'),
-- seq=2: New replacement meter, 10/07 to 31/07 = 200 kWh
(13, 2, 'KH001', 'METER-01', '2026_07', 1,
 '2026-07-10 00:00:00', '2026-07-31 23:59:59',
 0.00, 200.00, FALSE, 99999.9, 200.00, 'VALIDATED', 'ORIGINAL', 'HANDHELD');
