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
      extraFilters?: Record<string, string>;
      rawFilterSuffix?: string;
    } = {}
  ): Promise<T[]> {
    const {
      select = '*',
      watermark,
      watermarkColumn = 'created_at',
      batchSize = 1000,
      orderBy = 'created_at',
      maxRecords,
      extraFilters,
      rawFilterSuffix,
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

      // Apply any extra filters (e.g., timestamp range for month-by-month sync)
      if (extraFilters) {
        for (const [key, value] of Object.entries(extraFilters)) {
          params.set(key, value);
        }
      }

      let url = `${this.url}/rest/v1/${table}?${params.toString()}`;
      // Append raw filter suffix directly to avoid URL encoding issues with PostgREST operators
      if (rawFilterSuffix) {
        url += `&${rawFilterSuffix}`;
      }

      const headers: Record<string, string> = {
        'apikey': this.serviceKey,
        'Authorization': `Bearer ${this.serviceKey}`,
        'Content-Type': 'application/json',
        'Range': `${offset}-${offset + batchSize - 1}`,
      };
      // Only request exact count when no raw filter is used
      // (count=exact on large tables with filters can cause statement timeout)
      if (!rawFilterSuffix) {
        headers['Prefer'] = 'count=exact';
      }

      const response = await fetch(url, { headers });

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
        // Only stop based on content-range if we know the exact total
        if (total !== '*' && offset + batchSize >= parseInt(total, 10)) {
          hasMore = false;
        }
      }

      // Always stop if we got fewer records than requested (final page)
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

  /**
   * Fetch records for a specific month using cursor pagination with timestamp filter.
   * Uses small batch sizes (200) to avoid Supabase statement timeout.
   */
  async fetchByMonth<T extends { id: string }>(
    table: string,
    select: string,
    monthStart: string,
    monthEnd: string,
    batchSize: number = 200,
    maxRecords: number = 200000
  ): Promise<T[]> {
    console.log(`[${table}] Fetching ${monthStart} to ${monthEnd} (batch=${batchSize})...`);
    const allRecords: T[] = [];
    let lastId: string | null = null;
    let hasMore = true;
    let pageCount = 0;

    while (hasMore && allRecords.length < maxRecords) {
      let url = `${this.url}/rest/v1/${table}?select=${select}&order=id.asc&limit=${batchSize}`;
      url += `&timestamp=gte.${monthStart}&timestamp=lt.${monthEnd}`;
      if (lastId) {
        url += `&id=gt.${lastId}`;
      }

      const response = await fetch(url, {
        headers: {
          'apikey': this.serviceKey,
          'Authorization': `Bearer ${this.serviceKey}`,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        const error = await response.text();
        throw new Error(`Supabase fetchByMonth failed (${response.status}): ${error}`);
      }

      const records = await response.json() as T[];
      allRecords.push(...records);
      pageCount++;

      if (records.length < batchSize) {
        hasMore = false;
      } else {
        lastId = records[records.length - 1].id;
      }

      if (allRecords.length % 5000 < batchSize) {
        console.log(`[${table}] Fetched ${allRecords.length} records (${pageCount} pages)...`);
      }
    }

    console.log(`[${table}] Done: ${allRecords.length} records in ${pageCount} pages`);
    return allRecords;
  }
}
