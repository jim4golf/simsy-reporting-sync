#!/usr/bin/env node
/**
 * Rehash an admin user's password with 100k PBKDF2 iterations
 * (Cloudflare Workers Web Crypto limit).
 *
 * Usage: node scripts/rehash-password.mjs
 */

import { createInterface } from 'node:readline';
import { webcrypto, randomBytes } from 'node:crypto';

const subtle = globalThis.crypto?.subtle || webcrypto.subtle;
const PBKDF2_ITERATIONS = 100_000;
const SALT_BYTES = 16;
const HASH_BYTES = 32;

function toBase64(buf) { return Buffer.from(buf).toString('base64'); }
function fromBase64(b64) { return new Uint8Array(Buffer.from(b64, 'base64')); }

async function hashPassword(password, salt) {
  const encoder = new TextEncoder();
  const saltBytes = fromBase64(salt);
  const keyMaterial = await subtle.importKey('raw', encoder.encode(password), 'PBKDF2', false, ['deriveBits']);
  const bits = await subtle.deriveBits({ name: 'PBKDF2', salt: saltBytes, iterations: PBKDF2_ITERATIONS, hash: 'SHA-256' }, keyMaterial, HASH_BYTES * 8);
  return toBase64(bits);
}

const rl = createInterface({ input: process.stdin, output: process.stderr });

function ask(q) { return new Promise(resolve => rl.question(q, resolve)); }

async function main() {
  const email = await ask('Email [support@s-imsy.com]: ') || 'support@s-imsy.com';
  const password = await ask('New password (min 12 chars): ');
  if (!password || password.length < 12) {
    console.error('Password must be at least 12 characters.');
    process.exit(1);
  }
  rl.close();

  const salt = toBase64(randomBytes(SALT_BYTES));
  const hash = await hashPassword(password, salt);

  const sql = `UPDATE auth_users SET password_hash = '${hash}', salt = '${salt}', password_changed_at = now() WHERE email_lower = '${email.toLowerCase()}';`;
  console.log(sql);
  console.error('\nSQL written to stdout. Run on Hetzner:');
  console.error('  sudo -u postgres psql -d simsy_reporting -c "PASTE_SQL_HERE"');
}

main().catch(e => { console.error(e); process.exit(1); });
