/**
 * Supabase REST client for the Sync Worker.
 * Reads data from Supabase using the service role key with pagination.
 */

export class SupabaseClient {
  private url: string;
  private serviceKey: string;

  constructor(url: string, serviceKey: string) {
    this.url = url.replace(/\/$/, '');
    this.serviceKey = serviceKey;
  }

  /**
   * Fetch records from a Supabase table with pagination and optional watermark filtering.
   * Uses the Range header for server-side pagination.
   *
   * @param table - Table name
   * @param options.select - Columns to select (PostgREST select syntax)
   * @param options.watermark - Only fetch records created after this timestamp
   * @param options.watermarkColumn - Column to filter on (default: 'created_at')
   * @param options.batchSize - Records per page (default: 1000)
   * @param options.orderBy - Column to order by (default: 'created_at')
   * @param options.maxRecords - Maximum total records to fetch (default: unlimited)
   */
  async fetchAll<T>(
    table: string,
    options: {
      select?: string;
      watermark?: string | null;
      watermarkColumn?: string;
      batchSize?: number;
      orderBy?: string;
      maxRecords?: number;
    } = {}
  ): Promise<T[]> {
    const {
      select = '*',
      watermark,
      watermarkColumn = 'created_at',
      batchSize = 1000,
      orderBy = 'created_at',
      maxRecords,
    } = options;

    const allRecords: T[] = [];
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      // Build URL with query parameters
      const params = new URLSearchParams();
      params.set('select', select);
      params.set('order', `${orderBy}.asc`);

      if (watermark) {
        params.set(watermarkColumn, `gt.${watermark}`);
      }

      const url = `${this.url}/rest/v1/${table}?${params.toString()}`;

      const response = await fetch(url, {
        headers: {
          'apikey': this.serviceKey,
          'Authorization': `Bearer ${this.serviceKey}`,
          'Content-Type': 'application/json',
          'Range': `${offset}-${offset + batchSize - 1}`,
          'Prefer': 'count=exact',
        },
      });

      if (!response.ok) {
        const error = await response.text();
        throw new Error(`Supabase fetch ${table} failed (${response.status}): ${error}`);
      }

      const records = await response.json() as T[];
      allRecords.push(...records);

      // Check if we've hit the maxRecords limit
      if (maxRecords && allRecords.length >= maxRecords) {
        console.log(`[${table}] Reached maxRecords limit (${maxRecords}), stopping fetch`);
        hasMore = false;
        break;
      }

      // Check if there are more records
      const contentRange = response.headers.get('content-range');
      if (contentRange) {
        // Format: "0-999/5000" or "0-999/*"
        const match = contentRange.match(/\/(\d+|\*)/);
        const total = match ? match[1] : '*';
        if (total === '*' || offset + batchSize >= parseInt(total, 10)) {
          hasMore = false;
        }
      }

      if (records.length < batchSize) {
        hasMore = false;
      }

      offset += batchSize;

      // Safety: don't loop forever
      if (offset > 1000000) {
        console.warn(`[${table}] Safety limit reached at offset ${offset}`);
        hasMore = false;
      }
    }

    return allRecords;
  }
}
