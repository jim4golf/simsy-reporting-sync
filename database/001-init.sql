-- ============================================================================
-- S-IMSY Multi-Tenant Reporting Platform — Database Initialisation
-- ============================================================================
-- Run this script as the PostgreSQL superuser (postgres) on the Hetzner server.
--
-- Usage:
--   sudo -u postgres psql -f 001-init.sql
--
-- This script creates:
--   1. Two roles (simsy_reporting owner, simsy_reporting_app reader)
--   2. The simsy_reporting database
--   3. All reporting tables with Row-Level Security
--   4. Materialised views for pre-computed aggregates
--   5. Initial tenant data
-- ============================================================================

-- ============================================================================
-- STEP 1: Create Roles
-- ============================================================================

-- Owner role: used by the Sync Worker to INSERT/UPDATE data.
-- As the table owner, it bypasses RLS (correct for bulk sync).
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'simsy_reporting') THEN
        CREATE ROLE simsy_reporting WITH LOGIN PASSWORD 'CHANGE_ME_SYNC_PASSWORD';
    END IF;
END
$$;

-- Reader role: used by the API Worker to query data.
-- Subject to RLS — enforces tenant isolation at the database level.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'simsy_reporting_app') THEN
        CREATE ROLE simsy_reporting_app WITH LOGIN PASSWORD 'CHANGE_ME_APP_PASSWORD';
    END IF;
END
$$;

-- ============================================================================
-- STEP 2: Create Database
-- ============================================================================

-- NOTE: CREATE DATABASE cannot run inside a transaction block.
-- If running via psql -f, this works fine. If running inside a transaction,
-- execute this statement separately first:
--
--   CREATE DATABASE simsy_reporting OWNER simsy_reporting;
--
-- Then connect to it and run the rest:
--
--   \c simsy_reporting

SELECT 'DATABASE CREATION: Run the following manually if not already created:' AS notice;
SELECT 'CREATE DATABASE simsy_reporting OWNER simsy_reporting;' AS command;
SELECT 'Then connect: \\c simsy_reporting' AS next_step;

-- ============================================================================
-- STEP 3: Connect to the reporting database and create schema
-- ============================================================================
-- After creating the database, connect to it and run everything below.
-- \c simsy_reporting

-- ============================================================================
-- STEP 4: Tenant Registry
-- ============================================================================

CREATE TABLE IF NOT EXISTS rpt_tenants (
    tenant_id         TEXT PRIMARY KEY,
    tenant_name       TEXT NOT NULL,
    parent_tenant_id  TEXT REFERENCES rpt_tenants(tenant_id),
    role              TEXT NOT NULL CHECK (role IN ('tenant', 'customer')),
    is_active         BOOLEAN DEFAULT true,
    created_at        TIMESTAMPTZ DEFAULT now(),
    updated_at        TIMESTAMPTZ DEFAULT now()
);

-- Initial tenants
INSERT INTO rpt_tenants (tenant_id, tenant_name, role) VALUES
    ('allsee',        'Allsee Technologies Limited', 'tenant'),
    ('cellular-lan',  'Cellular-Lan',                'tenant'),
    ('simsy-app',     'SIMSY_application',           'tenant'),
    ('travel-simsy',  'Travel-SIMSY',                'tenant')
ON CONFLICT (tenant_id) DO NOTHING;

-- Initial customers
INSERT INTO rpt_tenants (tenant_id, tenant_name, parent_tenant_id, role) VALUES
    ('eclipse', 'Eclipse', 'simsy-app', 'customer')
ON CONFLICT (tenant_id) DO NOTHING;

-- ============================================================================
-- STEP 5: Usage Records (Sanitised)
-- ============================================================================

CREATE TABLE IF NOT EXISTS rpt_usage (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id             TEXT,                     -- event_id from Supabase for dedup
    tenant_id             TEXT NOT NULL REFERENCES rpt_tenants(tenant_id),
    customer_name         TEXT,
    endpoint_name         TEXT,
    endpoint_description  TEXT,
    iccid                 TEXT,
    timestamp             TIMESTAMPTZ,
    usage_date            DATE,
    service_type          TEXT,
    charge_type           TEXT,
    consumption           BIGINT,
    charged_consumption   BIGINT,
    uplink_bytes          BIGINT,
    downlink_bytes        BIGINT,
    bundle_name           TEXT,
    bundle_moniker        TEXT,
    status_moniker        TEXT,
    rat_type_moniker      TEXT,
    serving_operator_name TEXT,
    serving_country_name  TEXT,
    serving_country_iso2  TEXT,
    buy_charge            NUMERIC(12,4),
    buy_currency          TEXT,
    sell_charge           NUMERIC(12,4),
    sell_currency         TEXT,
    synced_at             TIMESTAMPTZ DEFAULT now()
);

-- Deduplication: one row per source event
CREATE UNIQUE INDEX IF NOT EXISTS idx_rpt_usage_source
    ON rpt_usage (source_id) WHERE source_id IS NOT NULL;

-- Query indexes
CREATE INDEX IF NOT EXISTS idx_rpt_usage_tenant
    ON rpt_usage (tenant_id);
CREATE INDEX IF NOT EXISTS idx_rpt_usage_tenant_ts
    ON rpt_usage (tenant_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_rpt_usage_tenant_iccid
    ON rpt_usage (tenant_id, iccid);
CREATE INDEX IF NOT EXISTS idx_rpt_usage_tenant_customer
    ON rpt_usage (tenant_id, customer_name);
CREATE INDEX IF NOT EXISTS idx_rpt_usage_tenant_date
    ON rpt_usage (tenant_id, usage_date);

-- ============================================================================
-- STEP 6: Active Bundles
-- ============================================================================

CREATE TABLE IF NOT EXISTS rpt_bundles (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id         TEXT,                     -- bundle_id from Supabase
    tenant_id         TEXT NOT NULL REFERENCES rpt_tenants(tenant_id),
    bundle_name       TEXT,
    bundle_moniker    TEXT,
    description       TEXT,
    price             NUMERIC(12,4),
    currency          TEXT,
    formatted_price   TEXT,
    allowance         BIGINT,
    allowance_moniker TEXT,
    bundle_type_name  TEXT,
    offer_type_name   TEXT,
    status_name       TEXT,
    effective_from    TEXT,
    effective_to      TEXT,
    synced_at         TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rpt_bundles_source
    ON rpt_bundles (source_id) WHERE source_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rpt_bundles_tenant
    ON rpt_bundles (tenant_id);
CREATE INDEX IF NOT EXISTS idx_rpt_bundles_tenant_status
    ON rpt_bundles (tenant_id, status_name);

-- ============================================================================
-- STEP 7: Bundle Instances
-- ============================================================================

CREATE TABLE IF NOT EXISTS rpt_bundle_instances (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id         TEXT,                     -- composite key for dedup
    tenant_id         TEXT NOT NULL REFERENCES rpt_tenants(tenant_id),
    customer_name     TEXT,
    endpoint_name     TEXT,
    iccid             TEXT,
    bundle_name       TEXT,
    bundle_moniker    TEXT,
    bundle_instance_id TEXT,
    start_time        TIMESTAMPTZ,
    end_time          TIMESTAMPTZ,
    status_name       TEXT,
    status_moniker    TEXT,
    sequence          INTEGER,
    sequence_max      INTEGER,
    data_used_mb      BIGINT,
    data_allowance_mb BIGINT,
    synced_at         TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rpt_bi_source
    ON rpt_bundle_instances (source_id) WHERE source_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rpt_bi_tenant
    ON rpt_bundle_instances (tenant_id);
CREATE INDEX IF NOT EXISTS idx_rpt_bi_tenant_iccid
    ON rpt_bundle_instances (tenant_id, iccid);
CREATE INDEX IF NOT EXISTS idx_rpt_bi_tenant_status
    ON rpt_bundle_instances (tenant_id, status_name);
CREATE INDEX IF NOT EXISTS idx_rpt_bi_tenant_expiry
    ON rpt_bundle_instances (tenant_id, end_time);

-- ============================================================================
-- STEP 8: Endpoints (Sanitised — no ICCID, IMSI, IP, lat/lon, keys)
-- ============================================================================

CREATE TABLE IF NOT EXISTS rpt_endpoints (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id             TEXT,                   -- endpoint_identifier from Supabase
    tenant_id             TEXT NOT NULL REFERENCES rpt_tenants(tenant_id),
    customer_id           TEXT,
    endpoint_name         TEXT,
    endpoint_type         TEXT,
    endpoint_type_name    TEXT,
    status                TEXT,
    endpoint_status_name  TEXT,
    network_status_name   TEXT,
    usage_rolling_24h     BIGINT,
    usage_rolling_7d      BIGINT,
    usage_rolling_28d     BIGINT,
    usage_rolling_1y      BIGINT,
    charge_rolling_24h    NUMERIC(12,4),
    charge_rolling_7d     NUMERIC(12,4),
    charge_rolling_28d    NUMERIC(12,4),
    charge_rolling_1y     NUMERIC(12,4),
    first_activity        TIMESTAMPTZ,
    latest_activity       TIMESTAMPTZ,
    synced_at             TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rpt_endpoints_source
    ON rpt_endpoints (source_id, tenant_id) WHERE source_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rpt_endpoints_tenant
    ON rpt_endpoints (tenant_id);

-- ============================================================================
-- STEP 9: Row-Level Security
-- ============================================================================

-- Enable RLS on all data tables
ALTER TABLE rpt_usage            ENABLE ROW LEVEL SECURITY;
ALTER TABLE rpt_bundles          ENABLE ROW LEVEL SECURITY;
ALTER TABLE rpt_bundle_instances ENABLE ROW LEVEL SECURITY;
ALTER TABLE rpt_endpoints        ENABLE ROW LEVEL SECURITY;

-- The owner role (simsy_reporting) bypasses RLS automatically.
-- The reader role (simsy_reporting_app) is subject to these policies.

-- Force RLS even for the table owner if we want extra safety
-- (commented out — the sync worker needs to write across tenants)
-- ALTER TABLE rpt_usage FORCE ROW LEVEL SECURITY;

-- Tenant isolation policies
CREATE POLICY tenant_isolation_usage ON rpt_usage
    FOR ALL
    TO simsy_reporting_app
    USING (tenant_id = current_setting('app.current_tenant', true));

CREATE POLICY tenant_isolation_bundles ON rpt_bundles
    FOR ALL
    TO simsy_reporting_app
    USING (tenant_id = current_setting('app.current_tenant', true));

CREATE POLICY tenant_isolation_bi ON rpt_bundle_instances
    FOR ALL
    TO simsy_reporting_app
    USING (tenant_id = current_setting('app.current_tenant', true));

CREATE POLICY tenant_isolation_endpoints ON rpt_endpoints
    FOR ALL
    TO simsy_reporting_app
    USING (tenant_id = current_setting('app.current_tenant', true));

-- rpt_tenants: allow reader to see own row + children
ALTER TABLE rpt_tenants ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_tenants ON rpt_tenants
    FOR SELECT
    TO simsy_reporting_app
    USING (
        tenant_id = current_setting('app.current_tenant', true)
        OR parent_tenant_id = current_setting('app.current_tenant', true)
    );

-- ============================================================================
-- STEP 10: Materialised Views
-- ============================================================================

-- Daily usage aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_usage_daily AS
SELECT
    tenant_id,
    customer_name,
    usage_date AS day,
    service_type,
    SUM(consumption)                    AS total_consumption,
    SUM(charged_consumption)            AS total_charged,
    SUM(uplink_bytes)                   AS total_uplink,
    SUM(downlink_bytes)                 AS total_downlink,
    SUM(uplink_bytes + downlink_bytes)  AS total_bytes,
    SUM(buy_charge)                     AS total_buy,
    SUM(sell_charge)                    AS total_sell,
    COUNT(*)                            AS record_count
FROM rpt_usage
WHERE usage_date IS NOT NULL
GROUP BY tenant_id, customer_name, usage_date, service_type;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_pk
    ON mv_usage_daily (tenant_id, COALESCE(customer_name, ''), day, COALESCE(service_type, ''));
CREATE INDEX IF NOT EXISTS idx_mv_daily_tenant
    ON mv_usage_daily (tenant_id);

-- Monthly usage aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_usage_monthly AS
SELECT
    tenant_id,
    customer_name,
    date_trunc('month', usage_date)::DATE AS month,
    service_type,
    SUM(consumption)                    AS total_consumption,
    SUM(charged_consumption)            AS total_charged,
    SUM(uplink_bytes)                   AS total_uplink,
    SUM(downlink_bytes)                 AS total_downlink,
    SUM(uplink_bytes + downlink_bytes)  AS total_bytes,
    SUM(buy_charge)                     AS total_buy,
    SUM(sell_charge)                    AS total_sell,
    COUNT(*)                            AS record_count
FROM rpt_usage
WHERE usage_date IS NOT NULL
GROUP BY tenant_id, customer_name, date_trunc('month', usage_date), service_type;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_monthly_pk
    ON mv_usage_monthly (tenant_id, COALESCE(customer_name, ''), month, COALESCE(service_type, ''));
CREATE INDEX IF NOT EXISTS idx_mv_monthly_tenant
    ON mv_usage_monthly (tenant_id);

-- Annual usage aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_usage_annual AS
SELECT
    tenant_id,
    customer_name,
    date_trunc('year', usage_date)::DATE AS year,
    service_type,
    SUM(consumption)                    AS total_consumption,
    SUM(charged_consumption)            AS total_charged,
    SUM(uplink_bytes)                   AS total_uplink,
    SUM(downlink_bytes)                 AS total_downlink,
    SUM(uplink_bytes + downlink_bytes)  AS total_bytes,
    SUM(buy_charge)                     AS total_buy,
    SUM(sell_charge)                    AS total_sell,
    COUNT(*)                            AS record_count
FROM rpt_usage
WHERE usage_date IS NOT NULL
GROUP BY tenant_id, customer_name, date_trunc('year', usage_date), service_type;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_annual_pk
    ON mv_usage_annual (tenant_id, COALESCE(customer_name, ''), year, COALESCE(service_type, ''));
CREATE INDEX IF NOT EXISTS idx_mv_annual_tenant
    ON mv_usage_annual (tenant_id);

-- Bundle expiry view
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_bundle_expiry AS
SELECT
    tenant_id,
    customer_name,
    iccid,
    bundle_name,
    bundle_moniker,
    bundle_instance_id,
    status_name,
    status_moniker,
    start_time,
    end_time,
    data_used_mb,
    data_allowance_mb,
    synced_at
FROM rpt_bundle_instances
WHERE status_name IN ('Active', 'Pending', 'active', 'pending')
   OR status_moniker IN ('Active', 'Pending', 'active', 'pending');

CREATE INDEX IF NOT EXISTS idx_mv_expiry_tenant
    ON mv_bundle_expiry (tenant_id);
CREATE INDEX IF NOT EXISTS idx_mv_expiry_end
    ON mv_bundle_expiry (tenant_id, end_time);

-- ============================================================================
-- STEP 11: Grant Permissions
-- ============================================================================

-- Owner role: full access (already owns the tables if created by this role)
GRANT ALL ON ALL TABLES IN SCHEMA public TO simsy_reporting;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO simsy_reporting;

-- Reader role: SELECT only on data tables + materialised views
GRANT CONNECT ON DATABASE simsy_reporting TO simsy_reporting_app;
GRANT USAGE ON SCHEMA public TO simsy_reporting_app;
GRANT SELECT ON rpt_tenants TO simsy_reporting_app;
GRANT SELECT ON rpt_usage TO simsy_reporting_app;
GRANT SELECT ON rpt_bundles TO simsy_reporting_app;
GRANT SELECT ON rpt_bundle_instances TO simsy_reporting_app;
GRANT SELECT ON rpt_endpoints TO simsy_reporting_app;
GRANT SELECT ON mv_usage_daily TO simsy_reporting_app;
GRANT SELECT ON mv_usage_monthly TO simsy_reporting_app;
GRANT SELECT ON mv_usage_annual TO simsy_reporting_app;
GRANT SELECT ON mv_bundle_expiry TO simsy_reporting_app;

-- Allow reader to set session variables (needed for RLS)
-- This is allowed by default in PostgreSQL — no special grant needed.

-- ============================================================================
-- STEP 12: Sync Metadata Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS rpt_sync_log (
    id              SERIAL PRIMARY KEY,
    sync_type       TEXT NOT NULL,               -- 'usage', 'bundles', 'instances', 'endpoints'
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    records_synced  INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'running',       -- 'running', 'completed', 'failed'
    error_message   TEXT,
    watermark       TIMESTAMPTZ                   -- last created_at processed
);

GRANT SELECT, INSERT, UPDATE ON rpt_sync_log TO simsy_reporting;
GRANT USAGE ON SEQUENCE rpt_sync_log_id_seq TO simsy_reporting;

-- ============================================================================
-- DONE
-- ============================================================================

SELECT 'Database initialisation complete.' AS status;
SELECT 'Tables created: rpt_tenants, rpt_usage, rpt_bundles, rpt_bundle_instances, rpt_endpoints' AS tables;
SELECT 'Materialised views: mv_usage_daily, mv_usage_monthly, mv_usage_annual, mv_bundle_expiry' AS views;
SELECT 'RLS enabled on all data tables for role: simsy_reporting_app' AS security;
SELECT 'Remember to change the default passwords for both roles!' AS warning;
