/**
 * S-IMSY Reporting Sync Worker
 *
 * Cloudflare Worker with Cron Trigger that synchronises sanitised data
 * from Supabase into the PostgreSQL reporting database on Hetzner.
 *
 * Schedule: Every 6 hours (configurable in wrangler.toml)
 * Bindings: HYPERDRIVE (PostgreSQL), SYNC_KV (watermarks), secrets (Supabase key)
 */

import type { Env, SyncResult } from './types';
import { SupabaseClient } from './supabase-client';
import { createDbClient, backfillBundleUsage, refreshMaterialisedViews } from './db';
import { syncUsage } from './sync/usage';
import { syncBundles } from './sync/bundles';
import { syncInstances } from './sync/instances';
import { syncEndpoints } from './sync/endpoints';

export default {
  /**
   * Scheduled handler — triggered by Cron Trigger.
   * Also callable via `wrangler dev --test-scheduled` for local testing.
   */
  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext
  ): Promise<void> {
    console.log('=== S-IMSY Reporting Sync Started ===');
    console.log(`Trigger: ${controller.cron}`);
    console.log(`Time: ${new Date().toISOString()}`);

    const startTime = Date.now();
    const results: SyncResult[] = [];

    try {
      // Initialise clients
      const supabase = new SupabaseClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_KEY);
      const sql = createDbClient(env);
      const batchSize = parseInt(env.SYNC_BATCH_SIZE || '1000', 10);

      // Get watermarks from KV
      const watermarks = {
        usage: await env.SYNC_KV.get('sync:watermark:usage'),
        bundles: await env.SYNC_KV.get('sync:watermark:bundles'),
        instances: await env.SYNC_KV.get('sync:watermark:instances'),
        endpoints: await env.SYNC_KV.get('sync:watermark:endpoints'),
      };

      console.log('Watermarks:', JSON.stringify(watermarks));

      // Ensure master tenant exists first (sub-tenants reference it via parent_tenant_id)
      await sql.unsafe(`
        INSERT INTO rpt_tenants (tenant_id, tenant_name, role) VALUES
          ('s-imsy', 'S-IMSY', 'tenant')
        ON CONFLICT (tenant_id) DO NOTHING
      `);

      // Ensure all sub-tenants exist with correct parent
      await sql.unsafe(`
        INSERT INTO rpt_tenants (tenant_id, tenant_name, parent_tenant_id, role) VALUES
          ('allsee',       'Allsee Technologies Limited', 's-imsy', 'tenant'),
          ('cellular-lan', 'Cellular-Lan',                's-imsy', 'tenant'),
          ('simsy-app',    'SIMSY_application',           's-imsy', 'tenant'),
          ('travel-simsy', 'Travel-SIMSY',                's-imsy', 'tenant'),
          ('trvllr',       'Trvllr',                      's-imsy', 'tenant')
        ON CONFLICT (tenant_id) DO UPDATE SET
          parent_tenant_id = EXCLUDED.parent_tenant_id
      `);

      // Record sync start
      const now = new Date().toISOString();

      // Sync each table sequentially to stay within CPU limits
      // Order matters: endpoints first (for tenant resolution), then instances, then usage, then bundles

      // 1. Endpoints (needed for tenant resolution context)
      console.log('\n--- Syncing Endpoints ---');
      const endpointsResult = await syncEndpoints(supabase, sql, watermarks.endpoints, batchSize);
      results.push(endpointsResult);
      if (!endpointsResult.error && endpointsResult.recordsSynced > 0) {
        await env.SYNC_KV.put('sync:watermark:endpoints', now);
      }

      // 2. Bundle Instances
      console.log('\n--- Syncing Bundle Instances ---');
      const instancesResult = await syncInstances(supabase, sql, watermarks.instances, batchSize);
      results.push(instancesResult);
      if (!instancesResult.error && instancesResult.recordsSynced > 0) {
        await env.SYNC_KV.put('sync:watermark:instances', now);
      }

      // 3. Usage Records (largest table — uses chunked fetch + bulk insert)
      console.log('\n--- Syncing Usage Records ---');
      const usageResult = await syncUsage(
        supabase, sql, watermarks.usage, batchSize,
        async (wm: string) => { await env.SYNC_KV.put('sync:watermark:usage', wm); }
      );
      results.push(usageResult);
      if (!usageResult.error && usageResult.recordsSynced > 0) {
        // Watermark is saved inside syncUsage based on actual records processed
        // Only update the overall watermark if syncUsage didn't save one itself
      }

      // 4. Active Bundles (depends on instances being synced first for tenant mapping)
      console.log('\n--- Syncing Active Bundles ---');
      const bundlesResult = await syncBundles(supabase, sql, watermarks.bundles, batchSize);
      results.push(bundlesResult);
      if (!bundlesResult.error && bundlesResult.recordsSynced > 0) {
        await env.SYNC_KV.put('sync:watermark:bundles', now);
      }

      // 5. Backfill data_used_mb on bundle instances from charged_consumption
      console.log('\n--- Backfilling Bundle Data Usage ---');
      await backfillBundleUsage(sql);

      // 6. Refresh materialised views
      console.log('\n--- Refreshing Materialised Views ---');
      await refreshMaterialisedViews(sql);

      // Store last successful sync time
      await env.SYNC_KV.put('sync:last_run', now);

      // Log summary
      const totalDuration = Date.now() - startTime;
      const totalRecords = results.reduce((sum, r) => sum + r.recordsSynced, 0);
      const errors = results.filter((r) => r.error);

      console.log('\n=== Sync Summary ===');
      console.log(`Duration: ${(totalDuration / 1000).toFixed(1)}s`);
      console.log(`Total records synced: ${totalRecords}`);
      for (const r of results) {
        console.log(`  ${r.table}: ${r.recordsSynced} records (${(r.duration / 1000).toFixed(1)}s)${r.error ? ` ERROR: ${r.error}` : ''}`);
      }
      if (errors.length > 0) {
        console.error(`${errors.length} table(s) had errors`);
      }

      // Store sync results in KV for monitoring
      await env.SYNC_KV.put('sync:last_result', JSON.stringify({
        timestamp: now,
        duration: totalDuration,
        results,
        status: errors.length > 0 ? 'partial' : 'success',
      }), { expirationTtl: 86400 * 7 }); // Keep for 7 days

      // Close DB connection
      await sql.end();

    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      console.error(`=== Sync FAILED: ${msg} ===`);

      // Store failure in KV
      await env.SYNC_KV.put('sync:last_result', JSON.stringify({
        timestamp: new Date().toISOString(),
        duration: Date.now() - startTime,
        status: 'failed',
        error: msg,
        results,
      }), { expirationTtl: 86400 * 7 });
    }
  },

  /**
   * HTTP handler — provides a simple status endpoint and manual trigger.
   * Useful for checking sync status and triggering manual syncs.
   */
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/status') {
      const lastResult = await env.SYNC_KV.get('sync:last_result', 'json');
      const lastRun = await env.SYNC_KV.get('sync:last_run');

      return new Response(JSON.stringify({
        service: 'simsy-reporting-sync',
        lastRun,
        lastResult,
      }, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (url.pathname === '/trigger' && request.method === 'POST') {
      // Manual trigger — runs the sync in the background
      ctx.waitUntil(
        this.scheduled(
          { scheduledTime: Date.now(), cron: 'manual' } as ScheduledController,
          env,
          ctx
        )
      );

      return new Response(JSON.stringify({
        message: 'Sync triggered. Check /status for results.',
        triggeredAt: new Date().toISOString(),
      }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    return new Response('S-IMSY Reporting Sync Worker\n\nGET /status — View sync status\nPOST /trigger — Trigger manual sync', {
      status: 200,
      headers: { 'Content-Type': 'text/plain' },
    });
  },
};
