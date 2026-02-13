# S-IMSY Reporting Platform — Setup Guide

## Prerequisites

- Hetzner server (128.140.64.5) with SSH access
- Cloudflare account (will need Workers paid plan — $5/month)
- Supabase service role key
- Node.js 18+ installed locally
- Wrangler CLI installed: `npm install -g wrangler`

---

## Step 1: Install PostgreSQL on Hetzner

SSH into your Hetzner server:

```bash
ssh root@128.140.64.5
```

Install PostgreSQL:

```bash
# Update packages
apt update && apt upgrade -y

# Install PostgreSQL 16
apt install -y postgresql postgresql-contrib

# Start and enable the service
systemctl start postgresql
systemctl enable postgresql

# Verify it's running
systemctl status postgresql
```

### Configure PostgreSQL for remote access

Edit PostgreSQL config to listen on all interfaces:

```bash
# Find the config file
sudo -u postgres psql -c "SHOW config_file;"
# Usually: /etc/postgresql/16/main/postgresql.conf

# Edit it
nano /etc/postgresql/16/main/postgresql.conf
```

Change the `listen_addresses` line:

```
listen_addresses = '*'    # Was: 'localhost'
```

Allow connections from Cloudflare's IP ranges in pg_hba.conf:

```bash
nano /etc/postgresql/16/main/pg_hba.conf
```

Add at the end:

```
# Allow Cloudflare Hyperdrive connections (password auth)
host    simsy_reporting    simsy_reporting       0.0.0.0/0    scram-sha-256
host    simsy_reporting    simsy_reporting_app   0.0.0.0/0    scram-sha-256
```

> **Security note:** In production, restrict `0.0.0.0/0` to Cloudflare's IP ranges.
> See: https://www.cloudflare.com/ips/

Restart PostgreSQL:

```bash
systemctl restart postgresql
```

### Open the firewall for PostgreSQL

```bash
ufw allow 5432/tcp
```

### Create the database and run the init script

Upload the SQL script to the server:

```bash
# From your local machine:
scp database/001-init.sql root@128.140.64.5:/tmp/
```

Then on the server:

```bash
# Create the database first
sudo -u postgres psql -c "CREATE DATABASE simsy_reporting;"

# Edit the SQL file to set real passwords (replace CHANGE_ME_*)
nano /tmp/001-init.sql

# Run the init script
sudo -u postgres psql -d simsy_reporting -f /tmp/001-init.sql
```

**IMPORTANT:** Change the default passwords in the SQL file before running it:
- `CHANGE_ME_SYNC_PASSWORD` → Strong password for the sync worker
- `CHANGE_ME_APP_PASSWORD` → Different strong password for the API worker

### Verify the setup

```bash
# Connect as the app reader role
psql -h localhost -U simsy_reporting_app -d simsy_reporting

# Test RLS
SET LOCAL app.current_tenant = 'allsee';
SELECT * FROM rpt_tenants;
-- Should show Allsee row only (not other tenants)

SET LOCAL app.current_tenant = 'simsy-app';
SELECT * FROM rpt_tenants;
-- Should show S-IMSY + Eclipse (child customer)
```

---

## Step 2: Upgrade Cloudflare to Workers Paid Plan

1. Go to https://dash.cloudflare.com
2. Navigate to **Workers & Pages** → **Plans**
3. Upgrade to the **Workers Paid** plan ($5/month)
4. This enables:
   - Hyperdrive (PostgreSQL connection pooling)
   - Cron Triggers (scheduled Workers)
   - Higher CPU limits (15 minutes)
   - KV with higher limits

---

## Step 3: Create Cloudflare Resources

### Login to Wrangler

```bash
wrangler login
```

### Create KV Namespaces

```bash
# For the sync worker (watermarks, sync metadata)
wrangler kv namespace create SIMSY_SYNC_KV
# Note the ID — put it in simsy-reporting-sync/wrangler.toml

# For the API worker (tenant mappings, rate limiting)
wrangler kv namespace create SIMSY_TENANT_KV
# Note the ID — put it in simsy-reporting-api/wrangler.toml
```

### Create Hyperdrive Configurations

```bash
# For the sync worker (owner role — can write data)
wrangler hyperdrive create simsy-reporting-db-sync \
  --connection-string="postgres://simsy_reporting:YOUR_SYNC_PASSWORD@128.140.64.5:5432/simsy_reporting"

# For the API worker (reader role — subject to RLS)
wrangler hyperdrive create simsy-reporting-db-api \
  --connection-string="postgres://simsy_reporting_app:YOUR_APP_PASSWORD@128.140.64.5:5432/simsy_reporting"
```

Note both Hyperdrive IDs and update the respective `wrangler.toml` files.

---

## Step 4: Deploy the Sync Worker

```bash
cd simsy-reporting-sync

# Install dependencies
npm install

# Update wrangler.toml with your Hyperdrive ID and KV namespace ID

# Set the Supabase service key as a secret
wrangler secret put SUPABASE_SERVICE_KEY
# Paste your Supabase service role key when prompted

# Deploy
wrangler deploy

# Trigger a manual sync to test
curl -X POST https://simsy-reporting-sync.YOUR_SUBDOMAIN.workers.dev/trigger

# Check status
curl https://simsy-reporting-sync.YOUR_SUBDOMAIN.workers.dev/status
```

---

## Step 5: Deploy the API Worker

```bash
cd simsy-reporting-api

# Install dependencies
npm install

# Update wrangler.toml with your Hyperdrive ID and KV namespace ID

# Deploy
wrangler deploy
```

---

## Step 6: Configure Cloudflare Access

### Create an Access Application

1. Go to Cloudflare Dashboard → **Zero Trust** → **Access** → **Applications**
2. Click **Add an application** → **Self-hosted**
3. Configure:
   - **Application name:** S-IMSY Reporting API
   - **Session duration:** 24 hours
   - **Application domain:** `simsy-reporting-api.YOUR_SUBDOMAIN.workers.dev`
     (or your custom domain if configured)
4. Create an **Access Policy:**
   - **Policy name:** Service Token Authentication
   - **Action:** Service Auth
   - **Include rule:** Service Token

### Create Service Tokens

Go to **Zero Trust** → **Access** → **Service Auth** → **Service Tokens**

Create 5 tokens:

| Token Name | For |
|---|---|
| `allsee-reporting` | Allsee Technologies Limited |
| `cellular-lan-reporting` | Cellular-Lan |
| `simsy-app-reporting` | SIMSY_application (S-IMSY) |
| `travel-simsy-reporting` | Travel-SIMSY |
| `eclipse-reporting` | Eclipse (customer of S-IMSY) |

**Save each Client ID and Client Secret** — you'll need them for KV mappings and to give to tenants.

### Populate KV with Tenant Mappings

For each service token, add a KV entry:

```bash
# Allsee Technologies
wrangler kv key put --namespace-id=YOUR_TENANT_KV_ID \
  "token:CLIENT_ID_FOR_ALLSEE" \
  '{"tenant_id":"allsee","tenant_name":"Allsee Technologies Limited","role":"tenant"}'

# Cellular-Lan
wrangler kv key put --namespace-id=YOUR_TENANT_KV_ID \
  "token:CLIENT_ID_FOR_CELLULAR_LAN" \
  '{"tenant_id":"cellular-lan","tenant_name":"Cellular-Lan","role":"tenant"}'

# SIMSY_application
wrangler kv key put --namespace-id=YOUR_TENANT_KV_ID \
  "token:CLIENT_ID_FOR_SIMSY" \
  '{"tenant_id":"simsy-app","tenant_name":"SIMSY_application","role":"tenant"}'

# Travel-SIMSY
wrangler kv key put --namespace-id=YOUR_TENANT_KV_ID \
  "token:CLIENT_ID_FOR_TRAVEL" \
  '{"tenant_id":"travel-simsy","tenant_name":"Travel-SIMSY","role":"tenant"}'

# Eclipse (customer under S-IMSY)
wrangler kv key put --namespace-id=YOUR_TENANT_KV_ID \
  "token:CLIENT_ID_FOR_ECLIPSE" \
  '{"tenant_id":"simsy-app","tenant_name":"SIMSY_application","role":"customer","customer_id":"eclipse","customer_name":"Eclipse"}'
```

---

## Step 7: Test Everything

### Test the API directly

```bash
# Health check
curl https://simsy-reporting-api.YOUR_SUBDOMAIN.workers.dev/health

# API info
curl https://simsy-reporting-api.YOUR_SUBDOMAIN.workers.dev/api/v1

# Authenticated request (replace with real Client ID/Secret)
curl -H "CF-Access-Client-Id: YOUR_CLIENT_ID" \
     -H "CF-Access-Client-Secret: YOUR_CLIENT_SECRET" \
     https://simsy-reporting-api.YOUR_SUBDOMAIN.workers.dev/api/v1/usage/summary

# Test different endpoints
curl -H "CF-Access-Client-Id: YOUR_CLIENT_ID" \
     -H "CF-Access-Client-Secret: YOUR_CLIENT_SECRET" \
     "https://simsy-reporting-api.YOUR_SUBDOMAIN.workers.dev/api/v1/usage/records?page=1"

curl -H "CF-Access-Client-Id: YOUR_CLIENT_ID" \
     -H "CF-Access-Client-Secret: YOUR_CLIENT_SECRET" \
     https://simsy-reporting-api.YOUR_SUBDOMAIN.workers.dev/api/v1/bundles

curl -H "CF-Access-Client-Id: YOUR_CLIENT_ID" \
     -H "CF-Access-Client-Secret: YOUR_CLIENT_SECRET" \
     https://simsy-reporting-api.YOUR_SUBDOMAIN.workers.dev/api/v1/endpoints
```

### Test tenant isolation

Use different service tokens and verify each tenant only sees their own data.

### Test customer scoping

Use Eclipse's token and verify it only sees Eclipse-scoped data within S-IMSY.

### View sync logs

```bash
wrangler tail simsy-reporting-sync
```

---

## Step 8: Custom Domain (Optional)

If you want `reporting-api.simsy.io` instead of the workers.dev URL:

1. Add the domain to Cloudflare (if not already)
2. In your Worker settings, add a **Custom Domain** route
3. Update the Access Application domain to match

---

## Monitoring

### Check sync status

```bash
curl https://simsy-reporting-sync.YOUR_SUBDOMAIN.workers.dev/status
```

### View Worker logs

```bash
# Sync Worker
wrangler tail simsy-reporting-sync

# API Worker
wrangler tail simsy-reporting-api
```

### Manual sync trigger

```bash
curl -X POST https://simsy-reporting-sync.YOUR_SUBDOMAIN.workers.dev/trigger
```

---

## Adding a New Tenant

1. **Cloudflare Access:** Create a new service token
2. **KV:** Add the token→tenant mapping:
   ```bash
   wrangler kv key put --namespace-id=YOUR_TENANT_KV_ID \
     "token:NEW_CLIENT_ID" \
     '{"tenant_id":"new-tenant","tenant_name":"New Tenant Ltd","role":"tenant"}'
   ```
3. **PostgreSQL:** Insert into rpt_tenants:
   ```sql
   INSERT INTO rpt_tenants (tenant_id, tenant_name, role)
   VALUES ('new-tenant', 'New Tenant Ltd', 'tenant');
   ```
4. The sync worker will automatically pick up data for the new tenant on the next run.

## Adding a New Customer Under a Tenant

1. **Cloudflare Access:** Create a new service token
2. **KV:** Add with customer role:
   ```bash
   wrangler kv key put --namespace-id=YOUR_TENANT_KV_ID \
     "token:NEW_CLIENT_ID" \
     '{"tenant_id":"parent-tenant","tenant_name":"Parent Tenant","role":"customer","customer_id":"new-cust","customer_name":"New Customer"}'
   ```
3. **PostgreSQL:**
   ```sql
   INSERT INTO rpt_tenants (tenant_id, tenant_name, parent_tenant_id, role)
   VALUES ('new-cust', 'New Customer', 'parent-tenant', 'customer');
   ```
