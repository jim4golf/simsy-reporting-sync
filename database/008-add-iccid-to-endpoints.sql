-- Migration 008: Add iccid column to rpt_endpoints
--
-- ICCID was previously excluded from the endpoints sync as "sensitive",
-- but it's already stored in rpt_bundle_instances and rpt_usage on the
-- same database. ICCID is the primary SIM identifier and is needed for
-- endpoint display in the reporting frontend.

ALTER TABLE rpt_endpoints ADD COLUMN IF NOT EXISTS iccid TEXT;

CREATE INDEX IF NOT EXISTS idx_rpt_endpoints_iccid
    ON rpt_endpoints (iccid) WHERE iccid IS NOT NULL;
