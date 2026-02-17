-- ============================================================================
-- Migration 006: Add bundle sequence fields to rpt_usage
--
-- The custom_usage_reports table in Supabase contains bundle_instance_id,
-- sequence, and sequence_max fields that are essential for determining:
--   - Which bundle instance each usage event belongs to
--   - The current bundle sequence (month) within a multi-month bundle
--   - The maximum sequence (total months) for overage/depletion analysis
--
-- These are augmented fields populated from active_bundles/bundle_instances.
-- Without them, it's impossible to calculate per-bundle-instance usage totals
-- and determine overage or depletion within a billing period.
-- ============================================================================

-- Add new columns (safe to run multiple times — IF NOT EXISTS not needed for ADD COLUMN with IF NOT EXISTS)
ALTER TABLE rpt_usage ADD COLUMN IF NOT EXISTS bundle_instance_id TEXT;
ALTER TABLE rpt_usage ADD COLUMN IF NOT EXISTS sequence INTEGER;
ALTER TABLE rpt_usage ADD COLUMN IF NOT EXISTS sequence_max INTEGER;

-- Index for querying usage by bundle instance (needed for per-bundle usage aggregation)
CREATE INDEX IF NOT EXISTS idx_rpt_usage_bundle_instance
    ON rpt_usage (bundle_instance_id) WHERE bundle_instance_id IS NOT NULL;

-- Composite index for bundle-level usage analysis per tenant
CREATE INDEX IF NOT EXISTS idx_rpt_usage_tenant_bundle_instance
    ON rpt_usage (tenant_id, bundle_instance_id) WHERE bundle_instance_id IS NOT NULL;

-- Grant permissions to the app role
GRANT SELECT ON rpt_usage TO simsy_reporting_app;

-- Also update existing materialised views to include the new fields
-- (The views will pick up the new columns on next REFRESH)
