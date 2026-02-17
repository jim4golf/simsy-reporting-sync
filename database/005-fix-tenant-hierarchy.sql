-- Migration 005: Fix tenant hierarchy
--
-- S-IMSY is the master tenant. All others are sub-tenants of S-IMSY.
-- Eclipse is a direct customer of S-IMSY (not simsy-app).
--
-- This migration:
--   1. Creates s-imsy as the master tenant
--   2. Sets parent_tenant_id for all sub-tenants → s-imsy
--   3. Fixes eclipse's parent from simsy-app → s-imsy
--   4. Updates RLS policies to support hierarchy-aware access

-- 1. Create s-imsy master tenant if not exists
INSERT INTO rpt_tenants (tenant_id, tenant_name, role)
VALUES ('s-imsy', 'S-IMSY', 'tenant')
ON CONFLICT (tenant_id) DO NOTHING;

-- 2. Set all sub-tenants as children of s-imsy
UPDATE rpt_tenants
SET parent_tenant_id = 's-imsy',
    updated_at = now()
WHERE tenant_id IN ('allsee', 'cellular-lan', 'simsy-app', 'travel-simsy', 'trvllr')
  AND (parent_tenant_id IS NULL OR parent_tenant_id != 's-imsy');

-- 3. Fix eclipse — customer of S-IMSY, not simsy-app
UPDATE rpt_tenants
SET parent_tenant_id = 's-imsy',
    updated_at = now()
WHERE tenant_id = 'eclipse'
  AND parent_tenant_id = 'simsy-app';

-- 4. Fix any data that was wrongly assigned to simsy-app but belongs to s-imsy.
--    The sync used to map 'S-IMSY' → 'simsy-app'. After this migration,
--    resetting the sync watermarks and re-running will fix the tenant_id
--    via the ON CONFLICT DO UPDATE clauses. But we can also fix existing
--    endpoint records by matching customer_id values that belong to S-IMSY.
--
--    Known S-IMSY customers (from Supabase endpoints data):
--    Eclipse Digital, Pete Scott, Dave Locke a test, Alexis Harris,
--    Another Trail, Davies Exports, Dean Hutchinson, European Cargo,
--    Gary Casey, Julie Mann, Michel Portrait, Middleton Aggregates,
--    Nulitics, Pepperl and Fuchs, Scott Brenton, S-IMSY, S-IMSY App,
--    Chris Le Brocq
--
--    These will be corrected when the sync re-runs with the fixed mapping.
--    No manual data fix needed here — the sync ON CONFLICT will handle it.

-- 5. Update RLS policies to support parent-tenant hierarchy.
--    Admin users (app.current_tenant = '*') see everything.
--    S-IMSY users (app.current_tenant = 's-imsy') see s-imsy + all sub-tenants.
--    Sub-tenant users see only their own tenant_id.

-- rpt_usage
DROP POLICY IF EXISTS tenant_isolation_usage ON rpt_usage;
CREATE POLICY tenant_isolation_usage ON rpt_usage
    FOR ALL
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
        OR (
            -- Parent tenant can see all sub-tenant data
            EXISTS (
                SELECT 1 FROM rpt_tenants
                WHERE rpt_tenants.tenant_id = rpt_usage.tenant_id
                  AND rpt_tenants.parent_tenant_id = current_setting('app.current_tenant', true)
            )
        )
    );

-- rpt_bundles
DROP POLICY IF EXISTS tenant_isolation_bundles ON rpt_bundles;
CREATE POLICY tenant_isolation_bundles ON rpt_bundles
    FOR ALL
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
        OR (
            EXISTS (
                SELECT 1 FROM rpt_tenants
                WHERE rpt_tenants.tenant_id = rpt_bundles.tenant_id
                  AND rpt_tenants.parent_tenant_id = current_setting('app.current_tenant', true)
            )
        )
    );

-- rpt_bundle_instances
DROP POLICY IF EXISTS tenant_isolation_bi ON rpt_bundle_instances;
CREATE POLICY tenant_isolation_bi ON rpt_bundle_instances
    FOR ALL
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
        OR (
            EXISTS (
                SELECT 1 FROM rpt_tenants
                WHERE rpt_tenants.tenant_id = rpt_bundle_instances.tenant_id
                  AND rpt_tenants.parent_tenant_id = current_setting('app.current_tenant', true)
            )
        )
    );

-- rpt_endpoints
DROP POLICY IF EXISTS tenant_isolation_endpoints ON rpt_endpoints;
CREATE POLICY tenant_isolation_endpoints ON rpt_endpoints
    FOR ALL
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
        OR (
            EXISTS (
                SELECT 1 FROM rpt_tenants
                WHERE rpt_tenants.tenant_id = rpt_endpoints.tenant_id
                  AND rpt_tenants.parent_tenant_id = current_setting('app.current_tenant', true)
            )
        )
    );

-- rpt_tenants — parent can see sub-tenants
DROP POLICY IF EXISTS tenant_isolation_tenants ON rpt_tenants;
CREATE POLICY tenant_isolation_tenants ON rpt_tenants
    FOR SELECT
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
        OR parent_tenant_id = current_setting('app.current_tenant', true)
    );
