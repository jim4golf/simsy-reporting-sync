-- Migration 004: Allow admin users to see all tenant data
-- When app.current_tenant is set to '*', RLS policies allow access to all rows.

-- Drop and recreate each policy to include the admin wildcard

-- rpt_usage
DROP POLICY IF EXISTS tenant_isolation_usage ON rpt_usage;
CREATE POLICY tenant_isolation_usage ON rpt_usage
    FOR ALL
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
    );

-- rpt_bundles
DROP POLICY IF EXISTS tenant_isolation_bundles ON rpt_bundles;
CREATE POLICY tenant_isolation_bundles ON rpt_bundles
    FOR ALL
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
    );

-- rpt_bundle_instances
DROP POLICY IF EXISTS tenant_isolation_bi ON rpt_bundle_instances;
CREATE POLICY tenant_isolation_bi ON rpt_bundle_instances
    FOR ALL
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
    );

-- rpt_endpoints
DROP POLICY IF EXISTS tenant_isolation_endpoints ON rpt_endpoints;
CREATE POLICY tenant_isolation_endpoints ON rpt_endpoints
    FOR ALL
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
    );

-- rpt_tenants
DROP POLICY IF EXISTS tenant_isolation_tenants ON rpt_tenants;
CREATE POLICY tenant_isolation_tenants ON rpt_tenants
    FOR SELECT
    TO simsy_reporting_app
    USING (
        current_setting('app.current_tenant', true) = '*'
        OR tenant_id = current_setting('app.current_tenant', true)
        OR parent_tenant_id = current_setting('app.current_tenant', true)
    );
