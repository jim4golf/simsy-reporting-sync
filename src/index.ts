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
import { syncUsage, syncUsageByMonth, buildIccidTenantMap } from './sync/usage';
import { syncBundles } from './sync/bundles';
import { syncInstances } from './sync/instances';
import { syncEndpoints } from './sync/endpoints';
import { collectUsageReport, backfillMonth, listReports } from './sync/collect-usage';

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

      // Check for resync queue FIRST — if we're in resync mode, skip all other
      // sync tasks and focus entirely on the month resync. This keeps the cron
      // well within the 15-minute wall-clock limit for Cloudflare cron triggers.
      const resyncQueueRaw = await env.SYNC_KV.get('sync:resync_queue');
      const resyncQueue = resyncQueueRaw ? JSON.parse(resyncQueueRaw) as {
        months: string[];
        started: string;
        completed: string[];
        errors: Record<string, string>;
      } : null;

      if (resyncQueue) {
        // ==================== RESYNC MODE ====================
        // Process ONE month from the queue, then exit.
        // Skip endpoints/instances/bundles/collection to maximize time for usage resync.
        // MV refresh only happens after the FINAL month.
        const remaining = resyncQueue.months.filter(m => !resyncQueue.completed.includes(m));

        if (remaining.length > 0) {
          const monthKey = remaining[0];
          const [yearStr, monthStr] = monthKey.split('-');
          const year = parseInt(yearStr, 10);
          const month = parseInt(monthStr, 10);
          const firstDay = `${monthKey}-01T00:00:00`;
          const nextMonth = month === 12
            ? `${year + 1}-01-01T00:00:00`
            : `${year}-${String(month + 1).padStart(2, '0')}-01T00:00:00`;

          console.log(`\n=== RESYNC MODE: Processing ${monthKey} (${remaining.length} months remaining) ===`);
          const monthStartMs = Date.now();

          try {
            // Build ICCID→tenant maps once
            console.log('[RESYNC] Building ICCID→tenant lookup...');
            const iccidMaps = await buildIccidTenantMap(supabase);

            // Use batch size of 500 for faster processing (tested: ~3-4s per batch, well under 8s timeout)
            const result = await syncUsageByMonth(supabase, sql, firstDay, nextMonth, 500, iccidMaps);
            resyncQueue.completed.push(monthKey);
            console.log(`[RESYNC] ${monthKey}: ${result.recordsSynced} records in ${((Date.now() - monthStartMs) / 1000).toFixed(1)}s`);
            results.push({ table: `rpt_usage (resync ${monthKey})`, recordsSynced: result.recordsSynced, duration: Date.now() - monthStartMs, error: result.error });

            if (result.error) {
              resyncQueue.errors[monthKey] = result.error;
            }
          } catch (monthErr) {
            const msg = monthErr instanceof Error ? monthErr.message : String(monthErr);
            resyncQueue.errors[monthKey] = msg;
            resyncQueue.completed.push(monthKey); // Mark as attempted to avoid infinite retry
            console.error(`[RESYNC] ${monthKey} failed: ${msg}`);
            results.push({ table: `rpt_usage (resync ${monthKey})`, recordsSynced: 0, duration: Date.now() - monthStartMs, error: msg });
          }

          // Check if all months are done
          const stillRemaining = resyncQueue.months.filter(m => !resyncQueue.completed.includes(m));
          if (stillRemaining.length === 0) {
            // ALL MONTHS COMPLETE — refresh MVs and clean up
            console.log('[RESYNC] All months complete! Refreshing MVs, setting watermark, clearing queue.');
            await backfillBundleUsage(sql);
            await refreshMaterialisedViews(sql);
            await env.SYNC_KV.put('sync:watermark:usage', new Date().toISOString());
            await env.SYNC_KV.delete('sync:resync_queue');
          } else {
            // Save progress for next cron run
            await env.SYNC_KV.put('sync:resync_queue', JSON.stringify(resyncQueue));
            console.log(`[RESYNC] ${stillRemaining.length} months remaining: ${stillRemaining.join(', ')}`);
          }
        } else {
          // Queue exists but all months done — clean up
          console.log('[RESYNC] Queue complete, cleaning up.');
          await backfillBundleUsage(sql);
          await refreshMaterialisedViews(sql);
          await env.SYNC_KV.put('sync:watermark:usage', new Date().toISOString());
          await env.SYNC_KV.delete('sync:resync_queue');
        }
      } else {
        // ==================== NORMAL MODE ====================
        // Full incremental sync: endpoints → instances → usage → bundles → MVs

        // 1. Endpoints
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

        // 3. Usage Records
        if (!watermarks.usage) {
          // SAFETY: If watermark is null, skip usage sync.
          console.warn('\n--- Skipping Usage Sync: watermark is null ---');
          console.warn('[USAGE] Use POST /start-resync to queue a full resync.');
          results.push({ table: 'rpt_usage', recordsSynced: 0, duration: 0, error: 'Skipped: no watermark (use /start-resync)' });
        } else {
          console.log('\n--- Syncing Usage Records (incremental) ---');
          const usageResult = await syncUsage(
            supabase, sql, watermarks.usage, batchSize,
            async (wm: string) => { await env.SYNC_KV.put('sync:watermark:usage', wm); }
          );
          results.push(usageResult);
        }

        // 4. Active Bundles
        console.log('\n--- Syncing Active Bundles ---');
        const bundlesResult = await syncBundles(supabase, sql, watermarks.bundles, batchSize);
        results.push(bundlesResult);
        if (!bundlesResult.error && bundlesResult.recordsSynced > 0) {
          await env.SYNC_KV.put('sync:watermark:bundles', now);
        }

        // 5. Backfill data_used_mb on bundle instances
        console.log('\n--- Backfilling Bundle Data Usage ---');
        await backfillBundleUsage(sql);

        // 6. Refresh materialised views
        console.log('\n--- Refreshing Materialised Views ---');
        await refreshMaterialisedViews(sql);

        // 7. Collect endpoints & bundles via Edge Function (if enabled)
        const collectEnabled = await env.SYNC_KV.get('config:collect_reports');
        if (collectEnabled === 'true') {
          // 7a. Endpoints & bundles via report-collector Edge Function
          console.log('\n--- Collecting Endpoints & Bundles (Edge Function) ---');
          try {
            for (const phase of ['endpoints', 'bundles'] as const) {
              const collectResp = await fetch(`${env.SUPABASE_URL}/functions/v1/report-collector`, {
                method: 'POST',
                headers: {
                  'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
                  'Content-Type': 'application/json',
                },
                body: JSON.stringify({ phase }),
              });

              if (collectResp.ok) {
                const collectResult = await collectResp.json() as Record<string, unknown>;
                console.log(`[Collect] ${phase} completed:`, JSON.stringify(collectResult));
              } else {
                console.warn(`[Collect] ${phase} returned status ${collectResp.status}`);
              }
            }
          } catch (collectError) {
            const collectMsg = collectError instanceof Error ? collectError.message : String(collectError);
            console.warn(`[Collect] Endpoints/bundles collection failed (non-blocking): ${collectMsg}`);
          }

          // 7b. Custom usage report — runs directly in this worker
          console.log('\n--- Collecting Custom Usage Report ---');
          try {
            const usageCollectResult = await collectUsageReport(env);
            console.log(`[Collect] Usage report: ${usageCollectResult.status}`,
              usageCollectResult.records ? `(${usageCollectResult.records} records)` : '',
              usageCollectResult.message || '');
          } catch (usageCollectError) {
            const msg = usageCollectError instanceof Error ? usageCollectError.message : String(usageCollectError);
            console.warn(`[Collect] Usage report collection failed (non-blocking): ${msg}`);
          }
        } else {
          console.log('\n--- Report Collection: disabled ---');
        }
      } // end normal mode

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

    if (url.pathname === '/backfill' && request.method === 'POST') {
      // Backfill a specific month's usage data (synchronous — waits for result)
      // Usage: POST /backfill with JSON body { "year": 2025, "month": 7 }
      try {
        const body = await request.json() as { year?: number; month?: number };
        const year = body.year;
        const month = body.month;
        if (!year || !month || month < 1 || month > 12) {
          return new Response(JSON.stringify({ error: 'Provide year and month (1-12)' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' },
          });
        }

        // Run backfill synchronously so caller sees the result
        const result = await backfillMonth(env, year, month);

        // If collection succeeded, trigger sync in background
        // IMPORTANT: Use null watermark so all records (including backfilled) are synced.
        // Do NOT save the watermark — let the normal cron manage its own watermark
        // to avoid the backfill advancing it past other months' data.
        if (result.status === 'completed' && result.records && result.records > 0) {
          ctx.waitUntil((async () => {
            try {
              const supabase = new SupabaseClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_KEY);
              const sql = createDbClient(env);
              const batchSize = parseInt(env.SYNC_BATCH_SIZE || '1000', 10);
              // Use the current usage watermark so we only sync NEW records
              // (the backfilled ones), not the entire history from scratch.
              const currentWatermark = await env.SYNC_KV.get('sync:watermark:usage');
              // Reset watermark to force a full re-sync that includes backfilled data
              // Use maxRecords override of 750k to handle full dataset in one pass
              await syncUsage(supabase, sql, null, batchSize, undefined, 750000);
              await backfillBundleUsage(sql);
              await refreshMaterialisedViews(sql);
              await sql.end();
              // Restore the watermark so cron picks up from where it was
              if (currentWatermark) {
                await env.SYNC_KV.put('sync:watermark:usage', currentWatermark);
              }
              console.log(`[BACKFILL] Sync + MV refresh complete for ${year}-${String(month).padStart(2, '0')}`);
            } catch (syncErr) {
              console.error(`[BACKFILL] Post-collection sync failed: ${syncErr instanceof Error ? syncErr.message : String(syncErr)}`);
            }
          })());
        }

        return new Response(JSON.stringify(result, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    if (url.pathname === '/reset-watermark' && request.method === 'POST') {
      // Reset a specific sync watermark to force re-sync
      // Usage: POST /reset-watermark with JSON body { "table": "usage" }
      try {
        const body = await request.json() as { table?: string };
        const table = body.table || 'usage';
        const key = `sync:watermark:${table}`;
        await env.SYNC_KV.delete(key);
        return new Response(JSON.stringify({
          message: `Watermark '${key}' reset. Next sync will start from the beginning.`,
        }), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    if (url.pathname === '/check-months') {
      // Diagnostic: check what data exists in Supabase and PostgreSQL for each month
      // GET /check-months — shows row counts per month in both systems
      try {
        const months: string[] = [];
        const now = new Date();
        // Check July 2025 through current month
        for (let y = 2025; y <= now.getFullYear(); y++) {
          const startM = y === 2025 ? 7 : 1;
          const endM = y === now.getFullYear() ? now.getMonth() + 1 : 12;
          for (let m = startM; m <= endM; m++) {
            months.push(`${y}-${String(m).padStart(2, '0')}`);
          }
        }

        const results: Record<string, { supabase: number; postgres: number }> = {};

        for (const monthKey of months) {
          const [yearStr, monthStr] = monthKey.split('-');
          const year = parseInt(yearStr, 10);
          const month = parseInt(monthStr, 10);
          const firstDay = `${monthKey}-01T00:00:00`;
          const nextMonth = month === 12
            ? `${year + 1}-01-01T00:00:00`
            : `${year}-${String(month + 1).padStart(2, '0')}-01T00:00:00`;

          // Check Supabase
          let supabaseCount = 0;
          try {
            const sbUrl = `${env.SUPABASE_URL}/rest/v1/custom_usage_reports?select=id&timestamp=gte.${firstDay}&timestamp=lt.${nextMonth}&limit=1`;
            const sbResp = await fetch(sbUrl, {
              headers: {
                'apikey': env.SUPABASE_SERVICE_KEY,
                'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
                'Prefer': 'count=exact',
                'Range': '0-0',
              },
            });
            const contentRange = sbResp.headers.get('content-range');
            if (contentRange) {
              const match = contentRange.match(/\/(\d+)/);
              if (match) supabaseCount = parseInt(match[1], 10);
            }
          } catch { /* ignore */ }

          // Check PostgreSQL
          let pgCount = 0;
          try {
            const sql = createDbClient(env);
            const pgResult = await sql.unsafe(
              `SELECT COUNT(*) AS cnt FROM rpt_usage WHERE usage_date >= $1::date AND usage_date < $2::date`,
              [firstDay, nextMonth]
            );
            pgCount = Number(pgResult[0]?.cnt || 0);
            await sql.end();
          } catch { /* ignore */ }

          results[monthKey] = { supabase: supabaseCount, postgres: pgCount };
        }

        // Also show current watermark
        const watermark = await env.SYNC_KV.get('sync:watermark:usage');

        return new Response(JSON.stringify({
          watermark,
          months: results,
        }, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    if (url.pathname === '/cleanup-duplicates' && request.method === 'POST') {
      // Remove duplicate records from Supabase for months where multiple CSV reports were ingested.
      // For each month, keeps only the records from the report_id that was used by our backfill
      // (the "Auto-collection" reports) and deletes records from other report_ids.
      // Also cleans up PostgreSQL and refreshes materialised views.
      //
      // Usage: POST /cleanup-duplicates with JSON body { "year": 2026, "month": 1 }
      // Or omit body to scan and clean all months.
      try {
        const body = await request.json().catch(() => ({})) as { year?: number; month?: number };

        // Known report_ids from our Friday backfill (Auto-collection reports)
        // These are the ones we want to KEEP
        const keepReportIds: Record<string, string> = {
          '2025-07': '019c7d6e-9b20-7750-bbe7-134238996df9',
          '2025-08': '019c7c2f-e255-71c7-8265-d4e22f7e1b1e',
          '2025-09': '019c7c2f-e26a-74cf-99c1-5b557f91cd97',
          '2025-10': '019c7c2f-e262-7899-a9f9-c077b7bfe892',
          '2025-11': '019c7c2f-e271-7f64-a879-06b662e1158e',
          '2025-12': '019c7bca-294f-7dcf-8da8-95b569ed1242',
          '2026-01': '019c7be6-075c-7a58-89fc-7bdbc8ec03ab',
        };

        const monthsToClean: string[] = [];
        if (body.year && body.month) {
          monthsToClean.push(`${body.year}-${String(body.month).padStart(2, '0')}`);
        } else {
          monthsToClean.push(...Object.keys(keepReportIds));
        }

        const results: Record<string, { before: number; deleted: number; after: number; pgBefore: number; pgAfter: number }> = {};

        for (const monthKey of monthsToClean) {
          const reportId = keepReportIds[monthKey];
          if (!reportId) continue;

          const [yearStr, monthStr] = monthKey.split('-');
          const year = parseInt(yearStr, 10);
          const month = parseInt(monthStr, 10);
          const firstDay = `${monthKey}-01T00:00:00`;
          const nextMonth = month === 12
            ? `${year + 1}-01-01T00:00:00`
            : `${year}-${String(month + 1).padStart(2, '0')}-01T00:00:00`;

          // Count before
          let beforeCount = 0;
          const countUrl = `${env.SUPABASE_URL}/rest/v1/custom_usage_reports?select=id&timestamp=gte.${firstDay}&timestamp=lt.${nextMonth}`;
          const countResp = await fetch(countUrl, {
            headers: {
              'apikey': env.SUPABASE_SERVICE_KEY,
              'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
              'Prefer': 'count=exact',
              'Range': '0-0',
            },
          });
          const cr = countResp.headers.get('content-range');
          if (cr) {
            const m = cr.match(/\/(\d+)/);
            if (m) beforeCount = parseInt(m[1], 10);
          }

          // First, discover what distinct report_ids exist for this month
          // Sample from different offsets to catch all report_ids
          const discoverUrl = `${env.SUPABASE_URL}/rest/v1/custom_usage_reports?select=report_id&timestamp=gte.${firstDay}&timestamp=lt.${nextMonth}&limit=1000`;
          const discoverResp = await fetch(discoverUrl, {
            headers: {
              'apikey': env.SUPABASE_SERVICE_KEY,
              'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
            },
          });
          const sampleRecords = await discoverResp.json() as Array<{ report_id: string }>;
          const foundReportIds = [...new Set(sampleRecords.map(r => r.report_id))];
          console.log(`[CLEANUP] ${monthKey}: found report_ids: ${JSON.stringify(foundReportIds)}, keeping: ${reportId}`);

          // Delete records that are NOT from our kept report_id for this month
          let deleted = 0;
          for (const badReportId of foundReportIds.filter(id => id !== reportId)) {
            // Delete by report_id directly (more reliable than combining with timestamp filter)
            const deleteUrl = `${env.SUPABASE_URL}/rest/v1/custom_usage_reports?report_id=eq.${badReportId}`;
            const deleteResp = await fetch(deleteUrl, {
              method: 'DELETE',
              headers: {
                'apikey': env.SUPABASE_SERVICE_KEY,
                'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
                'Prefer': 'count=exact',
              },
            });

            const dcr = deleteResp.headers.get('content-range');
            if (dcr) {
              const dm = dcr.match(/\/(\d+)/);
              if (dm) deleted += parseInt(dm[1], 10);
            }
            console.log(`[CLEANUP] ${monthKey}: deleted report_id=${badReportId}, status=${deleteResp.status}, content-range=${dcr}`);
          }

          // Count after
          let afterCount = 0;
          const afterResp = await fetch(countUrl, {
            headers: {
              'apikey': env.SUPABASE_SERVICE_KEY,
              'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
              'Prefer': 'count=exact',
              'Range': '0-0',
            },
          });
          const acr = afterResp.headers.get('content-range');
          if (acr) {
            const am = acr.match(/\/(\d+)/);
            if (am) afterCount = parseInt(am[1], 10);
          }

          // Now clean PostgreSQL: delete records that don't match the kept Supabase source IDs
          // Simpler approach: delete all PG records for this month and let the next sync re-populate
          let pgBefore = 0;
          let pgAfter = 0;
          try {
            const sql = createDbClient(env);
            const pgCountBefore = await sql.unsafe(
              `SELECT COUNT(*) AS cnt FROM rpt_usage WHERE usage_date >= $1::date AND usage_date < $2::date`,
              [firstDay, nextMonth]
            );
            pgBefore = Number(pgCountBefore[0]?.cnt || 0);

            // Delete PG records for this month — they'll be re-synced from the cleaned Supabase data
            await sql.unsafe(
              `DELETE FROM rpt_usage WHERE usage_date >= $1::date AND usage_date < $2::date`,
              [firstDay, nextMonth]
            );

            const pgCountAfter = await sql.unsafe(
              `SELECT COUNT(*) AS cnt FROM rpt_usage WHERE usage_date >= $1::date AND usage_date < $2::date`,
              [firstDay, nextMonth]
            );
            pgAfter = Number(pgCountAfter[0]?.cnt || 0);
            await sql.end();
          } catch (pgErr) {
            console.error(`[CLEANUP] PG error for ${monthKey}: ${pgErr instanceof Error ? pgErr.message : String(pgErr)}`);
          }

          results[monthKey] = { before: beforeCount, deleted, after: afterCount, pgBefore, pgAfter };
        }

        // Reset watermark so next sync re-populates PostgreSQL from clean Supabase data
        await env.SYNC_KV.delete('sync:watermark:usage');

        // Trigger a sync + MV refresh in background
        ctx.waitUntil((async () => {
          try {
            const supabase = new SupabaseClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_KEY);
            const sql = createDbClient(env);
            const batchSize = parseInt(env.SYNC_BATCH_SIZE || '1000', 10);
            await syncUsage(supabase, sql, null, batchSize, undefined, 750000);
            await backfillBundleUsage(sql);
            await refreshMaterialisedViews(sql);
            await sql.end();
            console.log('[CLEANUP] Post-cleanup sync + MV refresh complete');
          } catch (syncErr) {
            console.error(`[CLEANUP] Post-cleanup sync failed: ${syncErr instanceof Error ? syncErr.message : String(syncErr)}`);
          }
        })());

        return new Response(JSON.stringify({
          message: 'Cleanup complete. Sync triggered in background to re-populate PostgreSQL.',
          results,
        }, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    if (url.pathname === '/reports') {
      // List all reports from the S-IMSY API (via tenant-api-proxy)
      // GET /reports — shows all report IDs and statuses
      try {
        const reports = await listReports(env);
        return new Response(JSON.stringify({
          count: reports.length,
          reports,
        }, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    if (url.pathname === '/purge-pg' && request.method === 'POST') {
      // SAFE: Deletes ALL usage records from PostgreSQL (does NOT touch Supabase),
      // resets the watermark, then re-syncs everything from Supabase and refreshes MVs.
      // This treats Supabase as the single source of truth.
      try {
        const sql = createDbClient(env);

        // Count before
        const beforeResult = await sql.unsafe(`SELECT COUNT(*) AS cnt FROM rpt_usage`);
        const beforeCount = Number(beforeResult[0]?.cnt || 0);

        // Delete all usage records from PostgreSQL
        await sql.unsafe(`DELETE FROM rpt_usage`);
        console.log(`[PURGE-PG] Deleted ${beforeCount} records from rpt_usage`);

        await sql.end();

        // Reset watermark so full re-sync happens
        await env.SYNC_KV.delete('sync:watermark:usage');

        // Trigger full re-sync in background
        ctx.waitUntil((async () => {
          try {
            const supabase = new SupabaseClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_KEY);
            const sqlBg = createDbClient(env);
            const batchSize = parseInt(env.SYNC_BATCH_SIZE || '1000', 10);
            console.log('[PURGE-PG] Starting full re-sync from Supabase...');
            await syncUsage(supabase, sqlBg, null, batchSize, undefined, 750000);
            await backfillBundleUsage(sqlBg);
            await refreshMaterialisedViews(sqlBg);
            await sqlBg.end();
            console.log('[PURGE-PG] Full re-sync + MV refresh complete');
          } catch (syncErr) {
            console.error(`[PURGE-PG] Re-sync failed: ${syncErr instanceof Error ? syncErr.message : String(syncErr)}`);
          }
        })());

        return new Response(JSON.stringify({
          message: `Purged ${beforeCount} records from PostgreSQL. Full re-sync from Supabase triggered in background.`,
          deletedFromPg: beforeCount,
        }, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    if (url.pathname === '/test-filter') {
      // Debug: test Supabase timestamp filter with different limits
      const limit = url.searchParams.get('limit') || '100';
      const testUrl = `${env.SUPABASE_URL}/rest/v1/custom_usage_reports?select=id&timestamp=gte.2025-07-01T00:00:00&timestamp=lt.2025-08-01T00:00:00&order=id.asc&limit=${limit}`;
      const startMs = Date.now();
      const testResp = await fetch(testUrl, {
        headers: {
          'apikey': env.SUPABASE_SERVICE_KEY,
          'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        },
      });
      const elapsed = Date.now() - startMs;
      const body = await testResp.text();
      const records = JSON.parse(body) as Array<{ id: string }>;
      return new Response(JSON.stringify({
        status: testResp.status,
        count: records.length,
        elapsed: `${elapsed}ms`,
        firstId: records[0]?.id,
        lastId: records[records.length - 1]?.id,
        url: testUrl,
      }, null, 2), { headers: { 'Content-Type': 'application/json' } });
    }

    if (url.pathname === '/set-watermark' && request.method === 'POST') {
      // Set watermark to a specific value to stop cron from retrying full-table queries
      const body = await request.json() as { watermark?: string; table?: string };
      const table = body.table || 'usage';
      const wm = body.watermark || new Date().toISOString();
      await env.SYNC_KV.put(`sync:watermark:${table}`, wm);
      return new Response(JSON.stringify({
        message: `Watermark set to ${wm} for ${table}`,
      }), { headers: { 'Content-Type': 'application/json' } });
    }

    if (url.pathname === '/start-resync' && request.method === 'POST') {
      // Queue a resync of all months. The cron will process one month at a time
      // using its 15-minute CPU budget (HTTP handlers only get 30 seconds).
      // Usage: POST /start-resync (queues Jul 2025 through current month)
      // Or:    POST /start-resync with JSON body { "year": 2025, "month": 7 } for a single month
      try {
        const body = await request.json().catch(() => ({})) as { year?: number; month?: number };

        const monthsToSync: string[] = [];

        if (body.year && body.month) {
          monthsToSync.push(`${body.year}-${String(body.month).padStart(2, '0')}`);
        } else {
          // Queue all months from July 2025 to now
          const now = new Date();
          for (let y = 2025; y <= now.getFullYear(); y++) {
            const startM = y === 2025 ? 7 : 1;
            const endM = y === now.getFullYear() ? now.getMonth() + 1 : 12;
            for (let m = startM; m <= endM; m++) {
              monthsToSync.push(`${y}-${String(m).padStart(2, '0')}`);
            }
          }
        }

        // Store the resync queue in KV
        await env.SYNC_KV.put('sync:resync_queue', JSON.stringify({
          months: monthsToSync,
          started: new Date().toISOString(),
          completed: [],
          errors: {},
        }));

        return new Response(JSON.stringify({
          message: `Resync queued for ${monthsToSync.length} month(s). The cron (every 30 min) will process one month per run.`,
          months: monthsToSync,
          estimatedCompletion: `~${monthsToSync.length * 30} minutes`,
        }, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    if (url.pathname === '/resync-status') {
      // Check the status of a queued resync
      try {
        const queue = await env.SYNC_KV.get('sync:resync_queue', 'json') as {
          months: string[];
          started: string;
          completed: string[];
          errors: Record<string, string>;
        } | null;

        if (!queue) {
          return new Response(JSON.stringify({
            status: 'idle',
            message: 'No resync in progress. Use POST /start-resync to begin.',
          }, null, 2), { headers: { 'Content-Type': 'application/json' } });
        }

        const remaining = queue.months.filter(m => !queue.completed.includes(m));

        return new Response(JSON.stringify({
          status: remaining.length > 0 ? 'in_progress' : 'complete',
          started: queue.started,
          totalMonths: queue.months.length,
          completedMonths: queue.completed,
          remainingMonths: remaining,
          errors: queue.errors,
        }, null, 2), { headers: { 'Content-Type': 'application/json' } });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    if (url.pathname === '/resync-month' && request.method === 'POST') {
      // Sync a single month synchronously (for small months or testing).
      // For bulk resync, use POST /start-resync instead (uses cron's 15-min CPU budget).
      // Usage: POST /resync-month with JSON body { "year": 2025, "month": 7 }
      try {
        const body = await request.json() as { year?: number; month?: number };
        if (!body.year || !body.month) {
          return new Response(JSON.stringify({ error: 'Provide { year, month }' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' },
          });
        }

        const { year, month } = body;
        const monthKey = `${year}-${String(month).padStart(2, '0')}`;
        const firstDay = `${monthKey}-01T00:00:00`;
        const nextMonth = month === 12
          ? `${year + 1}-01-01T00:00:00`
          : `${year}-${String(month + 1).padStart(2, '0')}-01T00:00:00`;

        const supabase = new SupabaseClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_KEY);
        const sql = createDbClient(env);

        console.log(`[RESYNC-MONTH] Syncing ${monthKey}...`);
        const startMs = Date.now();

        const result = await syncUsageByMonth(supabase, sql, firstDay, nextMonth, 200);

        // Refresh MVs
        await backfillBundleUsage(sql);
        await refreshMaterialisedViews(sql);
        await sql.end();

        return new Response(JSON.stringify({
          month: monthKey,
          synced: result.recordsSynced,
          duration: `${((Date.now() - startMs) / 1000).toFixed(1)}s`,
          error: result.error || null,
        }, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return new Response(JSON.stringify({ error: msg }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
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

    return new Response('S-IMSY Reporting Sync Worker\n\nGET /status — View sync status\nPOST /trigger — Trigger manual sync\nPOST /purge-pg — Purge PostgreSQL and re-sync from Supabase', {
      status: 200,
      headers: { 'Content-Type': 'text/plain' },
    });
  },
};
