/**
 * Sync: bundle_instances_report (Supabase) → rpt_bundle_instances (PostgreSQL)
 *
 * Strips: raw_data, activation codes, scope fields
 * Keeps: tenant/customer info, bundle info, ICCID, time range, status, sequence
 *
 * Uses bulk INSERT with multi-row VALUES for performance.
 */

import type postgres from 'postgres';
import type { SupabaseClient } from '../supabase-client';
import type { SupabaseBundleInstanceRecord, SyncResult } from '../types';
import { resolveTenantId, buildBundleInstanceSourceId } from '../sanitise';

const SELECT_COLUMNS = [
  'id', 'created_at',
  'tenant_id', 'tenant_name', 'customer_id', 'customer_name',
  'endpoint_name', 'iccid',
  'bundle_name', 'bundle_moniker', 'bundle_instance_id',
  'start_time', 'end_time',
  'status_name', 'status_moniker',
  'sequence', 'sequence_max',
  // Deliberately NOT selecting: raw_data, scope_*, source
].join(',');

const BULK_INSERT_SIZE = 500;

/** Escape a value for a raw SQL VALUES clause. */
function esc(v: string | number | null | undefined): string {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') return String(v);
  return `'${String(v).replace(/'/g, "''")}'`;
}

export async function syncInstances(
  supabase: SupabaseClient,
  sql: postgres.Sql,
  watermark: string | null,
  batchSize: number
): Promise<SyncResult> {
  const start = Date.now();
  let recordsSynced = 0;

  try {
    console.log(`[INSTANCES] Starting sync (watermark: ${watermark || 'none'})`);

    const records = await supabase.fetchAll<SupabaseBundleInstanceRecord>(
      'bundle_instances_report',
      {
        select: SELECT_COLUMNS,
        watermark,
        watermarkColumn: 'created_at',
        batchSize,
      }
    );

    console.log(`[INSTANCES] Fetched ${records.length} records from Supabase`);

    if (records.length === 0) {
      return { table: 'rpt_bundle_instances', recordsSynced: 0, duration: Date.now() - start };
    }

    // Map and filter records
    const mapped: string[] = [];
    for (const r of records) {
      const tenantId = resolveTenantId(r.tenant_name, r.tenant_id);
      if (!tenantId) continue;

      const sourceId = buildBundleInstanceSourceId(
        r.bundle_instance_id,
        r.iccid,
        r.start_time
      );

      mapped.push(`(${esc(sourceId)}, ${esc(tenantId)}, ${esc(r.customer_name)}, ${esc(r.endpoint_name)}, ${esc(r.iccid)}, ${esc(r.bundle_name)}, ${esc(r.bundle_moniker)}, ${esc(r.bundle_instance_id)}, ${esc(r.start_time)}, ${esc(r.end_time)}, ${esc(r.status_name)}, ${esc(r.status_moniker)}, ${esc(r.sequence)}, ${esc(r.sequence_max)}, NOW())`);
    }

    console.log(`[INSTANCES] Mapped ${mapped.length} records (${records.length - mapped.length} skipped — unknown tenant)`);

    if (mapped.length === 0) {
      return { table: 'rpt_bundle_instances', recordsSynced: 0, duration: Date.now() - start };
    }

    // Bulk insert in chunks
    for (let i = 0; i < mapped.length; i += BULK_INSERT_SIZE) {
      const chunk = mapped.slice(i, i + BULK_INSERT_SIZE);
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
          synced_at = NOW()
      `);

      recordsSynced += chunk.length;

      if (recordsSynced % 5000 === 0 || i + BULK_INSERT_SIZE >= mapped.length) {
        console.log(`[INSTANCES] Upserted ${recordsSynced} / ${mapped.length}`);
      }
    }

    console.log(`[INSTANCES] Sync complete: ${recordsSynced} records in ${((Date.now() - start) / 1000).toFixed(1)}s`);
    return { table: 'rpt_bundle_instances', recordsSynced, duration: Date.now() - start };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[INSTANCES] Sync failed: ${msg}`);
    return { table: 'rpt_bundle_instances', recordsSynced, duration: Date.now() - start, error: msg };
  }
}
