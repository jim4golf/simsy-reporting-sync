/**
 * Sync: active_bundles (Supabase) → rpt_bundle_instances + rpt_bundles (PostgreSQL)
 *
 * The active_bundles table contains per-endpoint bundle instance data:
 * tenant, customer, endpoint, ICCID, bundle name, status, dates, sequence.
 *
 * Phase 1: Write ALL rows into rpt_bundle_instances (the detail table).
 * Phase 2: Derive a deduplicated rpt_bundles catalog (one row per bundle per tenant).
 *
 * Uses bulk INSERT with multi-row VALUES for performance.
 */

import type postgres from 'postgres';
import type { SupabaseClient } from '../supabase-client';
import type { SupabaseBundleRecord, SyncResult } from '../types';
import { resolveTenantId, buildBundleInstanceSourceId } from '../sanitise';

// Select all instance-level columns from Supabase active_bundles
const SELECT_COLUMNS = [
  'id', 'bundle_id', 'bundle_name', 'bundle_moniker',
  'status_name', 'status_moniker',
  'tenant_name', 'customer_name', 'endpoint_name',
  'iccid',
  'start_time', 'end_time',
  'sequence', 'sequence_max',
  'collected_at',
].join(',');

const BULK_INSERT_SIZE = 500;

/** Escape a value for a raw SQL VALUES clause. */
function esc(v: string | number | null | undefined): string {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') return String(v);
  return `'${String(v).replace(/'/g, "''")}'`;
}

export async function syncBundles(
  supabase: SupabaseClient,
  sql: postgres.Sql,
  watermark: string | null,
  batchSize: number
): Promise<SyncResult> {
  const start = Date.now();
  let recordsSynced = 0;

  try {
    console.log(`[BUNDLES] Starting sync (watermark: ${watermark || 'none'})`);

    const records = await supabase.fetchAll<SupabaseBundleRecord>(
      'active_bundles',
      {
        select: SELECT_COLUMNS,
        watermark,
        watermarkColumn: 'collected_at',
        orderBy: 'collected_at',
        batchSize,
      }
    );

    console.log(`[BUNDLES] Fetched ${records.length} records from Supabase`);

    if (records.length === 0) {
      return { table: 'rpt_bundle_instances', recordsSynced: 0, duration: Date.now() - start };
    }

    // ── Phase 1: Write ALL rows to rpt_bundle_instances ──────────────

    const instanceRows: string[] = [];
    let skippedNoTenant = 0;

    for (const r of records) {
      const tenantId = resolveTenantId(r.tenant_name, null);
      if (!tenantId) {
        skippedNoTenant++;
        continue;
      }

      const sourceId = buildBundleInstanceSourceId(
        r.id,
        r.iccid,
        r.start_time
      );

      instanceRows.push(`(${esc(sourceId)}, ${esc(tenantId)}, ${esc(r.customer_name)}, ${esc(r.endpoint_name)}, ${esc(r.iccid)}, ${esc(r.bundle_name)}, ${esc(r.bundle_moniker)}, ${esc(r.id)}, ${esc(r.start_time)}, ${esc(r.end_time)}, ${esc(r.status_name)}, ${esc(r.status_moniker)}, ${esc(r.sequence)}, ${esc(r.sequence_max)}, NOW())`);
    }

    console.log(`[BUNDLES] Mapped ${instanceRows.length} instance rows (${skippedNoTenant} skipped — unknown tenant)`);

    if (instanceRows.length > 0) {
      for (let i = 0; i < instanceRows.length; i += BULK_INSERT_SIZE) {
        const chunk = instanceRows.slice(i, i + BULK_INSERT_SIZE);
        const valuesClauses = chunk.join(',\n');

        await sql.unsafe(`
          INSERT INTO rpt_bundle_instances (
            source_id, tenant_id, customer_name, endpoint_name, iccid,
            bundle_name, bundle_moniker, bundle_instance_id,
            start_time, end_time, status_name, status_moniker,
            sequence, sequence_max, synced_at
          ) VALUES ${valuesClauses}
          ON CONFLICT (source_id) WHERE source_id IS NOT NULL
          DO UPDATE SET
            status_name = EXCLUDED.status_name,
            status_moniker = EXCLUDED.status_moniker,
            end_time = EXCLUDED.end_time,
            sequence = EXCLUDED.sequence,
            sequence_max = EXCLUDED.sequence_max,
            synced_at = NOW()
        `);

        recordsSynced += chunk.length;

        if (recordsSynced % 5000 === 0 || i + BULK_INSERT_SIZE >= instanceRows.length) {
          console.log(`[BUNDLES] Instances upserted ${recordsSynced} / ${instanceRows.length}`);
        }
      }
    }

    // ── Phase 2: Derive rpt_bundles catalog (deduplicated) ───────────

    const seen = new Set<string>();
    const catalogRows: string[] = [];

    for (const r of records) {
      const tenantId = resolveTenantId(r.tenant_name, null);
      if (!tenantId) continue;

      const dedup = `${r.bundle_id}:${tenantId}`;
      if (seen.has(dedup)) continue;
      seen.add(dedup);

      catalogRows.push(`(${esc(r.bundle_id)}, ${esc(tenantId)}, ${esc(r.bundle_name)}, ${esc(r.bundle_moniker)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(r.status_name)}, ${esc(r.start_time)}, ${esc(r.end_time)}, NOW())`);
    }

    if (catalogRows.length > 0) {
      for (let i = 0; i < catalogRows.length; i += BULK_INSERT_SIZE) {
        const chunk = catalogRows.slice(i, i + BULK_INSERT_SIZE);
        const valuesClauses = chunk.join(',\n');

        await sql.unsafe(`
          INSERT INTO rpt_bundles (
            source_id, tenant_id, bundle_name, bundle_moniker, description,
            price, currency, formatted_price, allowance, allowance_moniker,
            bundle_type_name, offer_type_name, status_name,
            effective_from, effective_to, synced_at
          ) VALUES ${valuesClauses}
          ON CONFLICT (source_id) WHERE source_id IS NOT NULL
          DO UPDATE SET
            bundle_name = EXCLUDED.bundle_name,
            status_name = EXCLUDED.status_name,
            effective_to = EXCLUDED.effective_to,
            synced_at = NOW()
        `);
      }

      console.log(`[BUNDLES] Catalog: ${catalogRows.length} unique bundles derived`);
    }

    console.log(`[BUNDLES] Sync complete: ${recordsSynced} instances, ${catalogRows.length} catalog entries in ${((Date.now() - start) / 1000).toFixed(1)}s`);
    return { table: 'rpt_bundle_instances', recordsSynced, duration: Date.now() - start };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[BUNDLES] Sync failed: ${msg}`);
    return { table: 'rpt_bundle_instances', recordsSynced, duration: Date.now() - start, error: msg };
  }
}
