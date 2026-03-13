-- ============================================================================
-- Migration 007: Backfill NULL enrichment fields in rpt_usage
--
-- After a re-sync, some usage records may have had their enrichment fields
-- (customer_name, endpoint_name, bundle_name, bundle_moniker, status_moniker,
-- bundle_instance_id, sequence, sequence_max) overwritten with NULLs.
--
-- This script re-populates them by joining against rpt_bundle_instances,
-- matching on ICCID + timestamp within the bundle instance's start/end window.
-- ============================================================================

-- Update usage records that have NULL enrichment fields
-- by joining against bundle instances on ICCID + time range
UPDATE rpt_usage u
SET
  customer_name      = COALESCE(u.customer_name, bi.customer_name),
  endpoint_name      = COALESCE(u.endpoint_name, bi.endpoint_name),
  bundle_name        = COALESCE(u.bundle_name, bi.bundle_name),
  bundle_moniker     = COALESCE(u.bundle_moniker, bi.bundle_moniker),
  status_moniker     = COALESCE(u.status_moniker, bi.status_moniker),
  bundle_instance_id = COALESCE(u.bundle_instance_id, bi.bundle_instance_id),
  sequence           = COALESCE(u.sequence, bi.sequence),
  sequence_max       = COALESCE(u.sequence_max, bi.sequence_max)
FROM rpt_bundle_instances bi
WHERE u.iccid IS NOT NULL
  AND u.iccid = bi.iccid
  AND u.tenant_id = bi.tenant_id
  AND u.timestamp >= bi.start_time
  AND u.timestamp <= bi.end_time
  AND (
    u.customer_name IS NULL
    OR u.endpoint_name IS NULL
    OR u.bundle_name IS NULL
    OR u.bundle_moniker IS NULL
    OR u.status_moniker IS NULL
    OR u.bundle_instance_id IS NULL
    OR u.sequence IS NULL
    OR u.sequence_max IS NULL
  );

-- Report how many records were updated
-- (Run this separately to see the count)
-- SELECT COUNT(*) FROM rpt_usage WHERE customer_name IS NULL OR bundle_name IS NULL;
