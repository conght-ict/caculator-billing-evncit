-- =========================================================================
-- DATABASE INITIALIZATION SCRIPT FOR CALCULATOR BILLING SYSTEM
-- Path: /docker-entrypoint-initdb.d/init.sql
-- Description: Creates schemas, tables, indexes, and seeds test data.
-- =========================================================================

-- 1. Create Tariffs Definitions Table
CREATE TABLE tariff (
    tariff_code VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(20) NOT NULL -- STEPPING, FLAT
);

-- 2. Create Tariffs Step Details Table
CREATE TABLE tariff_detail (
    detail_id BIGSERIAL PRIMARY KEY,
    tariff_code VARCHAR(50) REFERENCES tariff(tariff_code) ON DELETE CASCADE,
    step INT NOT NULL,
    min_kwh DECIMAL(12, 2) NOT NULL,
    max_kwh DECIMAL(12, 2), -- Can be NULL for final step
    unit_price DECIMAL(12, 2) NOT NULL
);

-- 3. Create Accounts Table
CREATE TABLE account (
    account_id VARCHAR(50) PRIMARY KEY,
    book_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL, -- ACTIVE, INACTIVE
    norms_factor INT DEFAULT 1 NOT NULL
);

-- 4. Create Meter Points Table
CREATE TABLE meter_point (
    meter_point_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) REFERENCES account(account_id) ON DELETE CASCADE,
    tariff_code VARCHAR(50) REFERENCES tariff(tariff_code),
    status VARCHAR(20) NOT NULL -- ACTIVE, INACTIVE
);

-- 5. Create Meter Relations Table (Tree topology)
CREATE TABLE meter_relation (
    relation_id BIGSERIAL PRIMARY KEY,
    parent_id VARCHAR(50) REFERENCES meter_point(meter_point_id) ON DELETE CASCADE,
    child_id VARCHAR(50) REFERENCES meter_point(meter_point_id) ON DELETE CASCADE,
    relation_type VARCHAR(20) NOT NULL -- NETTING, AGGREGATION
);

-- 6. Create Meter Usage Table (Ingestion & Readings staging)
CREATE TABLE meter_usage (
    usage_id BIGINT NOT NULL,
    account_id VARCHAR(50) NOT NULL,
    meter_point_id VARCHAR(50) NOT NULL,
    billing_cycle_month VARCHAR(20) NOT NULL,
    from_date TIMESTAMP NOT NULL,
    to_date TIMESTAMP NOT NULL,
    start_index DECIMAL(12, 2) NOT NULL,
    end_index DECIMAL(12, 2) NOT NULL,
    consumption DECIMAL(12, 2) GENERATED ALWAYS AS (end_index - start_index) STORED,
    status VARCHAR(20) NOT NULL, -- PENDING_MANUAL, VALIDATED
    PRIMARY KEY (usage_id, billing_cycle_month)
);

-- 7. Create Billing Snapshot Table
CREATE TABLE billing_account_snapshot (
    snapshot_id VARCHAR(100) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,
    book_id VARCHAR(50) NOT NULL,
    billing_cycle_month VARCHAR(20) NOT NULL,
    calculation_version INT NOT NULL,
    config_data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. Create Invoices Table
CREATE TABLE bill_invoice (
    invoice_id VARCHAR(100) NOT NULL,
    account_id VARCHAR(50) NOT NULL,
    book_id VARCHAR(50) NOT NULL,
    billing_cycle_month VARCHAR(20) NOT NULL,
    total_amount_before_tax DECIMAL(12, 2) NOT NULL,
    tax_amount DECIMAL(12, 2) NOT NULL,
    total_amount_after_tax DECIMAL(12, 2) NOT NULL,
    idempotency_key VARCHAR(100) UNIQUE NOT NULL,
    billing_manifest JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (invoice_id, billing_cycle_month)
);

-- 9. Create Transactional Outbox Events Table
CREATE TABLE outbox_event (
    event_id UUID PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CREATE INDEXES FOR OPTIMAL SEARCH
CREATE INDEX idx_meter_point_account ON meter_point(account_id);
CREATE INDEX idx_meter_usage_check ON meter_usage(account_id, billing_cycle_month, status);
CREATE INDEX idx_snapshot_lookup ON billing_account_snapshot(account_id, billing_cycle_month);
CREATE INDEX idx_invoice_lookup ON bill_invoice(account_id, billing_cycle_month);

-- =========================================================================
-- SEED INITIAL TEST DATA
-- =========================================================================

-- Seed Tariffs
INSERT INTO tariff (tariff_code, name, type) VALUES
('TARIFF_SHBT_2026', 'Sinh hoạt bậc thang', 'STEPPING'),
('TARIFF_KDOANH_2026', 'Kinh doanh đồng giá', 'FLAT'),
('TARIFF_SX_2026', 'Sản xuất đồng giá', 'FLAT'),
('TARIFF_SX_BT', 'Sản xuất - Giờ Bình thường', 'FLAT'),
('TARIFF_SX_CD', 'Sản xuất - Giờ Cao điểm', 'FLAT'),
('TARIFF_SX_TD', 'Sản xuất - Giờ Thấp điểm', 'FLAT');

-- Seed Tariff Details (Stepping block price limits and units)
INSERT INTO tariff_detail (tariff_code, step, min_kwh, max_kwh, unit_price) VALUES
('TARIFF_SHBT_2026', 1, 0, 50, 1806.00),
('TARIFF_SHBT_2026', 2, 50, 100, 1866.00),
('TARIFF_SHBT_2026', 3, 100, 200, 2167.00),
('TARIFF_SHBT_2026', 4, 200, 300, 2729.00),
('TARIFF_SHBT_2026', 5, 300, 400, 3050.00),
('TARIFF_SHBT_2026', 6, 400, NULL, 3157.00),

('TARIFF_KDOANH_2026', 1, 0, NULL, 2500.00),
('TARIFF_SX_2026', 1, 0, NULL, 2000.00),
('TARIFF_SX_BT', 1, 0, NULL, 1800.00),
('TARIFF_SX_CD', 1, 0, NULL, 3200.00),
('TARIFF_SX_TD', 1, 0, NULL, 1100.00);

-- Seed Accounts
INSERT INTO account (account_id, book_id, customer_name, status, norms_factor) VALUES
('KH001', 'SO_01', 'Nguyen Van A', 'ACTIVE', 1), -- Fast Path SHBT (norms=1)
('KH002', 'SO_01', 'Tran Thi B', 'ACTIVE', 1), -- Fast Path Sản xuất (norms=1)
('KH003', 'SO_01', 'Cong ty C', 'ACTIVE', 3), -- Slow Path Mix (Netting, norms=3)
('KH004', 'SO_01', 'Ho gia dinh D (SHBT 3 giá)', 'ACTIVE', 1), -- 3-rate residential
('KH005', 'SO_01', 'Nha may E (Sản xuất TOU 3 giá)', 'ACTIVE', 1); -- 3-rate TOU industrial

-- Seed Meter Points
INSERT INTO meter_point (meter_point_id, account_id, tariff_code, status) VALUES
('METER-01', 'KH001', 'TARIFF_SHBT_2026', 'ACTIVE'),
('METER-02', 'KH002', 'TARIFF_SX_2026', 'ACTIVE'),
('METER-03-TONG', 'KH003', 'TARIFF_SHBT_2026', 'ACTIVE'),
('METER-03-PHU', 'KH003', 'TARIFF_KDOANH_2026', 'ACTIVE'),
-- KH004 Meter points
('METER-04-BT', 'KH004', 'TARIFF_SHBT_2026', 'ACTIVE'),
('METER-04-CD', 'KH004', 'TARIFF_SHBT_2026', 'ACTIVE'),
('METER-04-TD', 'KH004', 'TARIFF_SHBT_2026', 'ACTIVE'),
-- KH005 Meter points
('METER-05-BT', 'KH005', 'TARIFF_SX_BT', 'ACTIVE'),
('METER-05-CD', 'KH005', 'TARIFF_SX_CD', 'ACTIVE'),
('METER-05-TD', 'KH005', 'TARIFF_SX_TD', 'ACTIVE');

-- Seed Meter Relations (METER-03-PHU subtracts from METER-03-TONG)
INSERT INTO meter_relation (parent_id, child_id, relation_type) VALUES
('METER-03-TONG', 'METER-03-PHU', 'NETTING');

-- Seed Meter Readings for month 2026_06
INSERT INTO meter_usage (usage_id, account_id, meter_point_id, billing_cycle_month, from_date, to_date, start_index, end_index, status) VALUES
-- KH001 (Consumes 250 kWh SHBT)
(1, 'KH001', 'METER-01', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 1000.00, 1250.00, 'VALIDATED'),

-- KH002 (Consumes 10,000 kWh SX)
(2, 'KH002', 'METER-02', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 5000.00, 15000.00, 'VALIDATED'),

-- KH003 (Mix Node: TONG = 500 kWh, PHU = 100 kWh, Net TONG = 400 kWh)
(3, 'KH003', 'METER-03-TONG', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 2000.00, 2500.00, 'VALIDATED'),
(4, 'KH003', 'METER-03-PHU', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 500.00, 600.00, 'VALIDATED'),

-- KH004 (BT = 200 kWh, CD = 50 kWh, TD = 100 kWh -> Total 350 kWh SHBT stepping)
(5, 'KH004', 'METER-04-BT', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 1000.00, 1200.00, 'VALIDATED'),
(6, 'KH004', 'METER-04-CD', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 500.00, 550.00, 'VALIDATED'),
(7, 'KH004', 'METER-04-TD', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 300.00, 400.00, 'VALIDATED'),

-- KH005 (BT = 1000 kWh @ 1800, CD = 200 kWh @ 3200, TD = 500 kWh @ 1100 -> TOU prices applied)
(8, 'KH005', 'METER-05-BT', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 1000.00, 2000.00, 'VALIDATED'),
(9, 'KH005', 'METER-05-CD', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 500.00, 700.00, 'VALIDATED'),
(10, 'KH005', 'METER-05-TD', '2026_06', '2026-06-01 00:00:00', '2026-06-30 23:59:59', 300.00, 800.00, 'VALIDATED');

-- 10. Create Billing Calculation Log Table
CREATE TABLE IF NOT EXISTS billing_calculation_log (
    log_id UUID PRIMARY KEY,
    book_id VARCHAR(50) NOT NULL,
    account_id VARCHAR(50) NOT NULL,
    billing_cycle_month VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    input_data JSONB,
    output_data JSONB,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_calc_log_book ON billing_calculation_log(book_id);
CREATE INDEX IF NOT EXISTS idx_calc_log_account ON billing_calculation_log(account_id);

-- 11. Create Book Billing Run Table
CREATE TABLE IF NOT EXISTS book_billing_run (
    run_id UUID PRIMARY KEY,
    book_id VARCHAR(50) NOT NULL,
    billing_cycle_month VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    total_accounts INT DEFAULT 0,
    processed_accounts INT DEFAULT 0,
    success_accounts INT DEFAULT 0,
    failed_accounts INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_book_run UNIQUE (book_id, billing_cycle_month)
);

-- 12. Create Account Billing Status Partitioned Table
CREATE TABLE IF NOT EXISTS account_billing_status (
    account_id VARCHAR(50) NOT NULL,
    billing_cycle_month VARCHAR(20) NOT NULL,
    book_id VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    invoice_id VARCHAR(100),
    error_message TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, billing_cycle_month)
) PARTITION BY LIST (billing_cycle_month);

CREATE TABLE IF NOT EXISTS account_billing_status_default
PARTITION OF account_billing_status DEFAULT;

CREATE INDEX IF NOT EXISTS idx_acc_bill_status_book ON account_billing_status(book_id);

