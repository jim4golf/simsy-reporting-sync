/**
 * Sync: active_bundles (Supabase) → rpt_bundles (PostgreSQL)
 *
 * The active_bundles table is a global catalog — not tenant-scoped in Supabase.
 * We assign bundles to tenants based on which tenants have instances of those bundles.
 * For the initial sync, we assign all bundles to all tenants.
 *
 * Uses bulk INSERT with multi-row VALUES for performance.
 */

import type postgres from 'postgres';
import type { SupabaseClient } from '../supabase-client';
import type { SupabaseBundleRecord, SyncResult } from '../types';
import { resolveTenantId } from '../sanitise';

// Column names match the LIVE Supabase schema (from types.ts)
// active_bundles is actually a bundle instances view with tenant info
const SELECT_COLUMNS = [
  'id', 'bundle_id', 'bundle_name', 'bundle_moniker',
  'status_name', 'status_moniker',
  'tenant_name',
  'start_time', 'end_time',
  'collected_at',
  // Deliberately NOT selecting: imsi, iccid, endpoint_iccid, source
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
      return { table: 'rpt_bundles', recordsSynced: 0, duration: Date.now() - start };
    }

    // Deduplicate bundles by bundle_id + tenant — active_bundles may have multiple rows
    // per bundle (one per endpoint). We want one rpt_bundles row per bundle per tenant.
    const seen = new Set<string>();
    const mapped: string[] = [];

    for (const r of records) {
      const tenantId = resolveTenantId(r.tenant_name, null);
      if (!tenantId) continue;

      // Deduplicate by bundle_id + tenant_id
      const dedup = `${r.bundle_id}:${tenantId}`;
      if (seen.has(dedup)) continue;
      seen.add(dedup);

      mapped.push(`(${esc(r.bundle_id)}, ${esc(tenantId)}, ${esc(r.bundle_name)}, ${esc(r.bundle_moniker)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(null)}, ${esc(r.status_name)}, ${esc(r.start_time)}, ${esc(r.end_time)}, NOW())`);
    }

    console.log(`[BUNDLES] Mapped ${mapped.length} unique bundles (${records.length} raw, ${records.length - mapped.length} duplicates/skipped)`);

    if (mapped.length === 0) {
      return { table: 'rpt_bundles', recordsSynced: 0, duration: Date.now() - start };
    }

    // Bulk insert in chunks
    for (let i = 0; i < mapped.length; i += BULK_INSERT_SIZE) {
      const chunk = mapped.slice(i, i + BULK_INSERT_SIZE);
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

      recordsSynced += chunk.length;
    }

    console.log(`[BUNDLES] Sync complete: ${recordsSynced} records in ${((Date.now() - start) / 1000).toFixed(1)}s`);
    return { table: 'rpt_bundles', recordsSynced, duration: Date.now() - start };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[BUNDLES] Sync failed: ${msg}`);
    return { table: 'rpt_bundles', recordsSynced, duration: Date.now() - start, error: msg };
  }
}
