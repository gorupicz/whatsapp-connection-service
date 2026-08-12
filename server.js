/**
 * WhatsApp connection service (Baileys)
 *
 * A small, self-hosted HTTP service that keeps a persistent WhatsApp Web
 * connection alive and exposes it over a REST API: QR pairing, health/status,
 * sending messages, basic group management, and forwarding inbound messages to
 * a webhook. Runs on any host that supports long-lived WebSocket connections
 * (Railway, Render, Fly.io, a VPS, …).
 *
 * Ships with an auto-reconnect loop and an in-process watchdog that detects a
 * silently-dropped socket and re-establishes it. See README.md for setup.
 */
import cors from 'cors';
import express from 'express';
import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore
} from '@whiskeysockets/baileys';
import NodeCache from 'node-cache';
import { Boom } from '@hapi/boom';
import pino from 'pino';
import QRCode from 'qrcode';
import path from 'path';
import { promises as fs } from 'fs';
import crypto from 'crypto';
import http from 'http';

// Ensure crypto is available globally for Baileys
if (typeof globalThis.crypto === 'undefined') {
  globalThis.crypto = crypto;
}

const app = express();

app.use(cors({
  origin: process.env.ALLOWED_ORIGINS?.split(',') || '*',
  methods: ['GET', 'POST', 'OPTIONS'],
}));


// Health check endpoint must be registered BEFORE express.json() middleware
// to respond as quickly as possible
app.get('/', (req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'whatsapp-service',
    timestamp: new Date().toISOString() 
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Honest liveness check: reports the timestamp of the last `messages.upsert`
// event we received. /health and /status can return "connected:true" while
// the Baileys socket is silently dead (zombie WebSocket with no close frame).
// The only trustworthy signal is "did any message event actually fire
// recently?". External monitors should read `staleSeconds` and alert if it
// exceeds their threshold during active hours.
app.get('/liveness', (req, res) => {
  const now = Date.now();
  const lastAt = lastMessageReceivedAt;
  const staleSeconds = lastAt ? Math.floor((now - lastAt) / 1000) : null;
  res.json({
    connected: connectionState === 'open' && socket !== null,
    connectionState,
    socketInitialized: socket !== null,
    lastMessageReceivedAt: lastAt ? new Date(lastAt).toISOString() : null,
    staleSeconds,
    now: new Date(now).toISOString(),
    // Counters since process start — helps diagnose "why aren't messages
    // landing in DB when the socket looks alive". Key ratios:
    //  - inboundMessages high, emptyText high, forwarded low
    //      → socket alive but messages are media/stickers with no text.
    //  - upsertEvents high, inboundMessages 0
    //      → only fromMe echoes; real inbound isn't being delivered.
    //  - all zero → socket open but no events at all (zombie).
    ingest: { ...ingestCounters },
    lastEmptyTextSample,
  });
});

app.use(express.json());

// ---- Request authentication -------------------------------------------------
// Every endpoint registered below this point can act as the paired WhatsApp
// account: send messages, create groups, add participants, read pending join
// requests. Without a shared secret, anyone who learns this service's URL can
// do all of that. So the key is REQUIRED: a fresh deployment refuses to boot
// rather than come up silently wide open. The escape hatch is explicit and has
// to be typed on purpose.
//
// The health endpoints (`/`, `/health`, `/liveness`) are registered above and
// stay public deliberately — PaaS health probes cannot send custom headers.
const SERVICE_API_KEY = process.env.SERVICE_API_KEY || '';
const ALLOW_UNAUTHENTICATED = process.env.ALLOW_UNAUTHENTICATED === '1';

if (!SERVICE_API_KEY && !ALLOW_UNAUTHENTICATED) {
  console.error(
    '[Service] FATAL: SERVICE_API_KEY is not set, so every endpoint would be ' +
    'open to anyone who knows this URL — including sending messages and ' +
    'creating groups as the paired account. Set SERVICE_API_KEY to a long ' +
    'random string, or set ALLOW_UNAUTHENTICATED=1 if this service is ' +
    'genuinely unreachable from the internet.',
  );
  process.exit(1);
}

if (!SERVICE_API_KEY) {
  console.warn(
    '[Service] WARNING: running WITHOUT request authentication ' +
    '(ALLOW_UNAUTHENTICATED=1). Anyone who can reach this URL controls the ' +
    'paired WhatsApp account.',
  );
}

function apiKeyMatches(given) {
  const a = Buffer.from(given);
  const b = Buffer.from(SERVICE_API_KEY);
  // timingSafeEqual throws on a length mismatch, so compare lengths first. The
  // length of a rejected key is not worth protecting; its content is.
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

app.use((req, res, next) => {
  if (!SERVICE_API_KEY) return next();
  // A CORS preflight cannot carry the custom header by definition.
  if (req.method === 'OPTIONS') return next();

  const given = req.get('x-api-key') || '';
  if (!given || !apiKeyMatches(given)) {
    console.warn(`[Service] 401 ${req.method} ${req.path}`);
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }
  next();
});

const PORT = process.env.PORT || 3001;
const AUTH_DIR = process.env.AUTH_DIR || path.join(process.cwd(), 'whatsapp_auth');
const WHATSAPP_WEBHOOK_URL = process.env.WHATSAPP_WEBHOOK_URL;
const WHATSAPP_WEBHOOK_SECRET = process.env.WHATSAPP_WEBHOOK_SECRET;


// Log port immediately
console.log(`[Service] PORT environment variable: ${process.env.PORT || 'not set, using default 3001'}`);
console.log(`[Service] Will listen on port: ${PORT}`);

// Logger for Baileys internals.
// Keep this restrictive to avoid leaking cryptographic/session internals in logs.
const waLogger = pino({ level: process.env.BAILEYS_LOG_LEVEL || 'error' });

// Estado global del socket
let socket = null;
let connectionState = 'close';
let currentQRCode = null;
let isReconnecting = false;
let whatsappRefused = false;
let sessionInvalid401 = false; // Rastrear si la sesión es inválida (error 401)
// Last disconnect diagnostic: populated on every connection.update close event
let lastDisconnectInfo = null; // { code: number, message: string, at: string }
// Single pending reconnect timer. Only ONE reconnect may ever be scheduled at a
// time — orphaned stacked timers were creating duplicate sockets on the same
// creds, which WhatsApp answered with a 'conflict' (440) that killed the freshly
// paired session. See scheduleReconnect / cancelScheduledReconnect below.
let reconnectTimer = null;

// SSE clients for real-time browser ↔ service state sync
const sseClients = new Set();
function broadcastConnectionState() {
  if (!sseClients.size) return;
  const connected = connectionState === 'open' && !!socket?.user;
  const payload = JSON.stringify({
    connected,
    connectionState,
    hasQR: !!currentQRCode,
    phoneNumber: connected ? socket?.user?.id : null,
    whatsappBlocked: Boolean(whatsappRefused),
    refusedCode: whatsappRefused || null,
    lastDisconnect: lastDisconnectInfo,
  });
  const data = `data: ${payload}\n\n`;
  for (const client of [...sseClients]) {
    try { client.write(data); } catch { sseClients.delete(client); }
  }
}

// Liveness tracking: timestamp (ms) of the last `messages.upsert` event we
// actually processed. Baileys can enter a "zombie" state where the socket
// reports connectionState='open' but no messages are delivered; this flag,
// exposed via /liveness and watched by the watchdog below, is the only
// reliable signal that the socket is actually receiving data from WhatsApp.
let lastMessageReceivedAt = null;
// Watchdog bookkeeping
let lastWatchdogRecoveryAt = 0;

// Ingestion counters — exposed via /liveness for post-mortem analysis.
// Helps distinguish "socket receives nothing" from "socket receives media-only
// messages that all get filtered". Reset on process restart.
const ingestCounters = {
  upsertEvents: 0,         // total messages.upsert events fired
  inboundMessages: 0,      // non-fromMe messages seen
  fromMeMessages: 0,       // own/outbound echoes
  emptyText: 0,            // dropped: extractTextFromMessage returned ''
  duplicates: 0,           // dropped: already processed within 30s window
  forwarded: 0,            // POSTed to webhook
  pseudoJidSkipped: 0,     // dropped: status/broadcast/newsletter pseudo-JID (not a real conversation)
};
let lastEmptyTextSample = null; // diagnostic: keys of message types we're dropping
const groupNameCache = new Map(); // Cache para nombres de grupos
// Cache LID→phone mapping from join requests so we can resolve on 'add' events.
// Baileys emits 'request' with LID-based JIDs and 'add' with phone-based JIDs.
// Key: "groupId:phone@s.whatsapp.net" → LID JID
// Key: "groupId:lid@lid" → phone@s.whatsapp.net
const joinRequestLidCache = new Map();
const logIncomingGroupMessages = process.env.WHATSAPP_LOG_INCOMING_GROUP_MESSAGES !== '0';
const logOwnMessages = process.env.WHATSAPP_LOG_OWN_MESSAGES === '1';

// Cache for Signal protocol message retry counters.
// Prevents encryption/decryption loops across socket restarts.
const msgRetryCounterCache = new NodeCache();

/**
 * Extract plain text from a Baileys message object.
 * Handles text messages, image/video captions, and contact cards (vCards).
 */
function extractTextFromMessage (message) {
  if (!message) return '';
  if (message.conversation) return message.conversation;
  if (message.extendedTextMessage?.text) return message.extendedTextMessage.text;
  if (message.imageMessage?.caption) return message.imageMessage.caption;
  if (message.videoMessage?.caption) return message.videoMessage.caption;
  if (message.documentMessage?.caption) return message.documentMessage.caption;
  if (message.documentWithCaptionMessage?.message?.documentMessage?.caption)
    return message.documentWithCaptionMessage.message.documentMessage.caption;

  // Single contact card (vCard)
  if (message.contactMessage) {
    const { displayName, vcard } = message.contactMessage;
    const telMatch = vcard?.match(/TEL[^:\n]*:([^\n\r]+)/);
    const phone = telMatch ? telMatch[1].trim().replace(/\s+/g, '') : '';
    return `[Contacto compartido] ${displayName || ''}${phone ? ` — ${phone}` : ''}`.trim();
  }

  // Multiple contact cards
  if (message.contactsArrayMessage?.contacts?.length) {
    const lines = message.contactsArrayMessage.contacts.map(c => {
      const tel = c.vcard?.match(/TEL[^:\n]*:([^\n\r]+)/)?.[1]?.trim().replace(/\s+/g, '') || '';
      return `${c.displayName || ''}${tel ? ` — ${tel}` : ''}`;
    });
    return `[Contactos compartidos] ${lines.filter(Boolean).join(', ')}`;
  }

  // WhatsApp Business catalog product
  if (message.productMessage?.product) {
    const p = message.productMessage.product;
    const title = p.title || '';
    const description = p.description || '';
    const currency = p.currencyCode || '';
    const price = p.priceAmount1000 ? (p.priceAmount1000 / 1000).toLocaleString('es-MX') : '';
    const parts = ['[Producto de catálogo]'];
    if (title) parts.push(title);
    if (description) parts.push(description);
    if (price && currency) parts.push(`${price} ${currency}`);
    else if (price) parts.push(price);
    return parts.join(' — ');
  }

  return '';
}

/**
 * Extract quoted/replied-to text from a Baileys message object (best-effort).
 * This helps downstream services detect when a message is replying to a previous system message.
 */
function getContextInfo(message) {
  if (!message) return null;
  return (
    message.extendedTextMessage?.contextInfo ||
    message.contactMessage?.contextInfo ||
    message.contactsArrayMessage?.contextInfo ||
    message.imageMessage?.contextInfo ||
    message.videoMessage?.contextInfo ||
    message.productMessage?.contextInfo ||
    null
  );
}

function extractQuotedTextFromMessage(message) {
  if (!message) return null;
  const ctx = getContextInfo(message);
  const quoted = ctx?.quotedMessage;
  if (!quoted) return null;

  // quotedMessage can contain the same shapes as normal messages
  if (quoted.conversation) return quoted.conversation;
  if (quoted.extendedTextMessage?.text) return quoted.extendedTextMessage.text;
  if (quoted.imageMessage?.caption) return quoted.imageMessage.caption;
  if (quoted.videoMessage?.caption) return quoted.videoMessage.caption;
  if (quoted.productMessage?.product) return extractTextFromMessage(quoted);
  return null;
}

/** Baileys can return fileLength as a number, string, or Long — coerce to a number. */
function toFiniteNumber (v) {
  if (v == null) return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v.toNumber === 'function') {
    try { const n = v.toNumber(); return Number.isFinite(n) ? n : null; } catch { return null; }
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * True only for JIDs that represent a real, addressable conversation: a 1:1 DM
 * (`@s.whatsapp.net` or a privacy `@lid`) or a group (`@g.us`).
 *
 * WhatsApp also emits messages.upsert events for pseudo-JIDs that are NOT real
 * conversations — `status@broadcast` (Stories/Status), broadcast lists
 * (`…@broadcast`) and channels/newsletters (`…@newsletter`). These must never be
 * treated as real DMs: a Status you post yourself can otherwise surface here as a
 * media "DM" and get mis-handled.
 */
function isRealConversationJid (jid) {
  if (!jid) return false;
  return (
    jid.endsWith('@g.us') ||
    jid.endsWith('@s.whatsapp.net') ||
    jid.endsWith('@lid')
  );
}

async function forwardIncomingMessage (payload) {
  if (!WHATSAPP_WEBHOOK_URL) {
    console.log('[WhatsApp] WHATSAPP_WEBHOOK_URL not configured, skipping incoming message forward');
    return;
  }

  try {
    const res = await fetch(WHATSAPP_WEBHOOK_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-whatsapp-secret': WHATSAPP_WEBHOOK_SECRET || '',
      },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error('[WhatsApp] Failed to forward message:', res.status, text);
    }
  } catch (error) {
    console.error('[WhatsApp] Error forwarding incoming message:', error);
  }
}


/**
 * Borrar sesión de autenticación inválida
 * Esto es necesario cuando WhatsApp rechaza la sesión (error 401)
 */
async function clearAuthSession() {
  try {
    console.log('[WhatsApp] Borrando sesión de autenticación inválida...');
    
    // Cerrar socket existente si está abierto
    if (socket) {
      try {
        await socket.end();
      } catch (e) {
        // Ignorar errores al cerrar
      }
      socket = null;
    }
    
    // Borrar todos los archivos en el directorio de autenticación
    try {
      const files = await fs.readdir(AUTH_DIR);
      for (const file of files) {
        const filePath = path.join(AUTH_DIR, file);
        await fs.unlink(filePath);
        console.log('[WhatsApp] Archivo de sesión borrado:', file);
      }
      console.log('[WhatsApp] Sesión de autenticación borrada completamente');
    } catch (err) {
      // Si el directorio no existe o está vacío, está bien
      if (err.code !== 'ENOENT') {
        console.error('[WhatsApp] Error al borrar archivos de sesión:', err);
      }
    }
    
    // Resetear estado
    connectionState = 'close';
    currentQRCode = null;
    isReconnecting = false;
    sessionInvalid401 = false; // Reset flag de sesión inválida
    
    return true;
  } catch (error) {
    console.error('[WhatsApp] Error al borrar sesión:', error);
    return false;
  }
}

/**
 * Clean up Signal protocol auth files.
 *
 * Lesson learned (2026-04-20): purging live Signal keys breaks decryption for
 * peers that still reference them. Dropping a `session-<jid>` file causes
 * "No session record" errors on every future message from that device; dropping
 * a `pre-key-<N>` that a peer has cached causes "Invalid PreKey ID" on their
 * next pkmsg; dropping a `sender-key-` causes "Bad MAC" on future group
 * messages from that sender until SKDM rotation (which may be never).
 *
 * So we only prune categories Baileys regenerates transparently. Everything
 * else — session/sender-key/pre-key/sender-key-memory/device-list/lid-mapping
 * — we LEAVE ALONE and just log their counts so we can watch volume growth.
 */
const AUTH_CLEANUP_INTERVAL_MS = 24 * 60 * 60 * 1000; // Run once per day
let authCleanupTimer = null;

// Prefixes of files safe to prune and their per-prefix keep cap.
// Baileys regenerates these transparently with no visible effect on peers.
//
// pre-key-: Baileys generates in batches of 100; keeping 500 = 5 full batches
// of buffer. Pre-keys above that threshold were either never uploaded (service
// failed to connect) or have already been distributed and consumed.
// app-state-sync-version-: recent sync state only; 100 is plenty.
const AUTH_PRUNABLE_PREFIXES = [
  'app-state-sync-version-',
  'pre-key-',
];

const AUTH_PRUNABLE_KEEP = {
  'app-state-sync-version-': 100,
  'pre-key-': 500,
};

// Prefixes we must never delete automatically. Listed here so the cleanup
// log shows their growth over time — if one of these balloons, we'll see it.
const AUTH_PROTECTED_PREFIXES = [
  'session-',
  'sender-key-memory-',
  'sender-key-',
  'device-list-',
  'lid-mapping-',
];

async function cleanupAuthKeyFiles() {
  try {
    const files = await fs.readdir(AUTH_DIR);
    const protectedFiles = new Set(['creds.json']);

    // Longer prefixes first so "sender-key-memory-" matches before "sender-key-".
    const allKnownPrefixes = [...AUTH_PRUNABLE_PREFIXES, ...AUTH_PROTECTED_PREFIXES]
      .sort((a, b) => b.length - a.length);

    const buckets = new Map(); // prefix → [{file, mtime, size}]
    let otherCount = 0;

    for (const file of files) {
      if (protectedFiles.has(file)) continue;

      const matchedPrefix = allKnownPrefixes.find(prefix => file.startsWith(prefix));
      if (!matchedPrefix) {
        otherCount++;
        continue;
      }

      const filePath = path.join(AUTH_DIR, file);
      try {
        const stat = await fs.stat(filePath);
        if (!buckets.has(matchedPrefix)) buckets.set(matchedPrefix, []);
        buckets.get(matchedPrefix).push({ file, mtime: stat.mtimeMs, size: stat.size });
      } catch (err) {
        if (err.code !== 'ENOENT') {
          console.error(`[Cleanup] Error stat ${file}:`, err.message);
        }
      }
    }

    // Log distribution for all known prefixes (protected + prunable) so we
    // can monitor volume growth of the protected ones.
    const distribution = {};
    for (const [prefix, items] of buckets) {
      distribution[prefix.replace(/-$/, '')] = items.length;
    }
    distribution['other'] = otherCount;
    console.log('[Cleanup] File distribution:', JSON.stringify(distribution));

    // Only prune the whitelisted prefixes.
    let deletedCount = 0;
    let freedBytes = 0;

    for (const prefix of AUTH_PRUNABLE_PREFIXES) {
      const keep = AUTH_PRUNABLE_KEEP[prefix] ?? 500;
      const items = buckets.get(prefix);
      if (!items || items.length <= keep) continue;

      items.sort((a, b) => b.mtime - a.mtime); // newest first
      const toDelete = items.slice(keep);
      for (const { file, size } of toDelete) {
        try {
          await fs.unlink(path.join(AUTH_DIR, file));
          deletedCount++;
          freedBytes += size;
        } catch (err) {
          if (err.code !== 'ENOENT') {
            console.error(`[Cleanup] Error deleting ${file}:`, err.message);
          }
        }
      }

      console.log(`[Cleanup] ${prefix}*: kept ${keep}, deleted ${toDelete.length}`);
    }

    if (deletedCount > 0) {
      const freedMB = (freedBytes / (1024 * 1024)).toFixed(2);
      console.log(`[Cleanup] Total: deleted ${deletedCount} files, freed ${freedMB} MB`);
    } else {
      console.log('[Cleanup] No files to clean up');
    }

    try {
      const remainingFiles = await fs.readdir(AUTH_DIR);
      console.log(`[Cleanup] Auth directory now has ${remainingFiles.length} files`);
    } catch (_) { /* ignore */ }

  } catch (err) {
    if (err.code === 'ENOENT') {
      console.log('[Cleanup] Auth directory does not exist yet, skipping');
    } else {
      console.error('[Cleanup] Error during auth cleanup:', err);
    }
  }
}

function startAuthCleanupSchedule() {
  if (authCleanupTimer) return;

  // Run once on startup (after a short delay to let init finish)
  setTimeout(() => {
    cleanupAuthKeyFiles();
  }, 30_000);

  // Then run every 24 hours
  authCleanupTimer = setInterval(() => {
    cleanupAuthKeyFiles();
  }, AUTH_CLEANUP_INTERVAL_MS);

  const capSummary = AUTH_PRUNABLE_PREFIXES.map(p => `${p}*≤${AUTH_PRUNABLE_KEEP[p] ?? 500}`).join(', ');
  console.log(`[Cleanup] Scheduled auth key cleanup — ${capSummary}, interval: 24h`);
}

/**
 * Cancel any pending reconnect timer. Called whenever the socket makes forward
 * progress (connecting/open) or is about to be re-created, so a stale timer can
 * never fire a second initSocket() into an in-flight handshake.
 */
function cancelScheduledReconnect() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

/**
 * Schedule exactly ONE reconnect. Any previously-scheduled reconnect is
 * cancelled first, so reconnect attempts never stack into duplicate sockets.
 */
function scheduleReconnect(ms, reason) {
  cancelScheduledReconnect();
  console.log(`[WhatsApp] Reconnect scheduled in ${ms}ms (${reason})`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    console.log('[WhatsApp] Reintentando conexión...');
    initSocket();
  }, ms);
}

/**
 * Inicializar socket de WhatsApp
 */
async function initSocket() {
  // Never build a second socket while one is alive OR mid-handshake. The
  // 'connecting' guard is critical: WhatsApp's post-pairing 515 restart leaves a
  // paired-but-not-yet-open socket, and a second makeWASocket() on the same creds
  // during that window triggers a 'conflict' (440) that kills the pairing.
  if (socket && (connectionState === 'open' || connectionState === 'connecting')) {
    return socket;
  }

  if (isReconnecting) {
    return socket;
  }

  isReconnecting = true;
  // A fresh init supersedes any pending scheduled reconnect.
  cancelScheduledReconnect();

  try {
    // Asegurar que el directorio existe
    await fs.mkdir(AUTH_DIR, { recursive: true });

    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version } = await fetchLatestBaileysVersion();

    console.log('[WhatsApp] Auth directory:', AUTH_DIR);
    console.log('[WhatsApp] Baileys version:', version);

    socket = makeWASocket({
      logger: waLogger,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, waLogger),
      },
      version,
      browser: ['Ubuntu', 'Chrome', '22.04.4'],
      msgRetryCounterCache,
      // getMessage is required to activate WhatsApp's retry mechanism.
      // Without it, decryption failures (No SenderKeyRecord) are permanent and silent.
      // Returning undefined signals "not available locally" — WhatsApp will request
      // the sender to re-distribute their key, restoring decryption for that participant.
      getMessage: async (_key) => undefined,
    });

    socket.ev.on('creds.update', saveCreds);

    socket.ev.on('connection.update', (update) => {
      const { connection, lastDisconnect, qr } = update;
      // Only update connectionState when the event actually carries a state change.
      // Without this guard, QR-only events (connection=undefined) would incorrectly
      // reset connectionState to 'close' while Baileys is mid-handshake.
      if (connection) connectionState = connection;

      if (qr) {
        // aquí actualizas tu currentQRCode para el endpoint /qr
        currentQRCode = qr;
        console.log('[WhatsApp] Nuevo QR recibido');
        broadcastConnectionState();
      }

      if (connection === 'connecting') {
        // Baileys emits 'connecting' both on initial socket bring-up (just before
        // the QR) and on the post-scan reconnect. Clear any stale QR and broadcast
        // so the browser can mirror the phone's "Iniciando sesión" once a scan
        // clears the QR (the frontend shows the session state only when no QR is
        // pending, so the pre-QR 'connecting' doesn't cause a false positive).
        currentQRCode = null;
        // The socket is making forward progress — drop any pending reconnect so
        // it can't fire a competing initSocket() mid-handshake.
        cancelScheduledReconnect();
        console.log('[WhatsApp] 🔄 Vinculando dispositivo...');
        broadcastConnectionState();
      }

      if (connection === 'open') {
        console.log('[WhatsApp] ✅ Conectado');
        currentQRCode = null;
        cancelScheduledReconnect();
        isReconnecting = false;
        whatsappRefused = false; // Reset bloqueo cuando conecta exitosamente
        sessionInvalid401 = false; // Reset flag de sesión inválida cuando conecta exitosamente
        broadcastConnectionState();
      }

      if (connection === 'close') {
        const error = lastDisconnect?.error;
        const boom = error instanceof Boom ? error : undefined;
        const statusCode = boom?.output?.statusCode;

        lastDisconnectInfo = {
          code: statusCode || 0,
          message: boom?.message || error?.message || String(error) || 'unknown',
          at: new Date().toISOString(),
        };

        console.log('[WhatsApp] Conexión cerrada, statusCode:', statusCode);
        console.log('[WhatsApp] Disconnect summary:', {
          statusCode,
          name: error?.name || null,
          message: boom?.message || error?.message || String(error),
        });
        broadcastConnectionState();

        // 403 (forbidden) and 405 both mean WhatsApp is refusing this client,
        // not that the connection glitched. Reconnecting is actively harmful:
        // a rejected client hammering the auth endpoint is what turns a
        // temporary block into a longer or permanent one. Stop, stay stopped,
        // and make /status say so — recovery is a human decision.
        //
        // 403 in particular is what an account gets after behaviour WhatsApp
        // reads as automated abuse. It does NOT clear by retrying.
        if (statusCode === 403 || statusCode === 405) {
          whatsappRefused = statusCode;
          isReconnecting = false;
          cancelScheduledReconnect();
          console.error(
            `[WhatsApp] Error ${statusCode}: WhatsApp está rechazando esta cuenta. ` +
            'No se reconectará automáticamente. Reintentar alarga el bloqueo.',
          );
          return; // Salir sin reconectar
        }

        // Detectar error 401 (Unauthorized) - sesión inválida
        // Necesitamos borrar la sesión y generar un nuevo QR code
        if (statusCode === 401) {
          console.log('[WhatsApp] Error 401 detectado - Sesión inválida. Borrando sesión para generar nuevo QR...');
          isReconnecting = false;
          cancelScheduledReconnect();
          sessionInvalid401 = true; // Marcar que la sesión es inválida
          
          // Borrar sesión inválida de forma asíncrona
          clearAuthSession().then(() => {
            console.log('[WhatsApp] Sesión borrada. Se puede llamar a /init para generar nuevo QR code.');
            sessionInvalid401 = false; // Reset después de borrar
          }).catch((err) => {
            console.error('[WhatsApp] Error al borrar sesión:', err);
          });
          
          return; // No reconectar automáticamente, esperar a que se llame /init
        }

        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        isReconnecting = false; // marcamos que ya terminó este intento

        if (shouldReconnect) {
          // 515 (restartRequired) is the EXPECTED restart right after a successful
          // pairing — reconnect promptly so the login completes. Everything else
          // (transient drops, 440 conflict, 408) backs off a bit. Either way it's
          // a SINGLE scheduled reconnect (scheduleReconnect cancels any prior one),
          // so we can't spawn competing sockets.
          const delayMs = statusCode === 515 ? 1500 : 5000;
          scheduleReconnect(delayMs, `close statusCode=${statusCode ?? 'unknown'}`);
        } else {
          console.log('[WhatsApp] Sesión cerrada definitivamente, borrar auth_info');
          cancelScheduledReconnect();
          // Cuando es loggedOut, también borrar la sesión
          clearAuthSession().catch((err) => {
            console.error('[WhatsApp] Error al borrar sesión:', err);
          });
        }
      }
    });

    // ✅ FIX: Track processed messages to prevent duplicate forwarding
    // Use a Map with TTL to track messages we've already forwarded
    const processedMessages = new Map(); // key: `${remoteJid}_${key.id || textHash}`, value: timestamp
    
    // Listen for incoming messages and forward to Next API
    socket.ev.on('messages.upsert', async ({ messages, type }) => {
      if (!messages || messages.length === 0) return;

      ingestCounters.upsertEvents += 1;

      // Liveness must ONLY count real inbound messages from other users.
      // Counting empty/fromMe/notify-only upserts means the bot's own
      // outbound echoes keep the zombie detector fooled indefinitely.
      const hasInboundMessage = messages.some(
        m => m?.key && m.key.fromMe !== true && m.message
      );
      if (hasInboundMessage) {
        lastMessageReceivedAt = Date.now();
      }

      for (const msg of messages) {
        try {
          const { key, message, pushName, messageTimestamp } = msg;
          if (!key || !message) continue;
          if (key.fromMe === true) {
            ingestCounters.fromMeMessages += 1;
          } else {
            ingestCounters.inboundMessages += 1;
          }

          const remoteJid = key.remoteJid || '';

          // Pseudo-JIDs (WhatsApp Status, broadcast lists, channels) are NOT real
          // conversations — never treat them as DMs. Skip them outright.
          if (!isRealConversationJid(remoteJid)) {
            ingestCounters.pseudoJidSkipped += 1;
            continue;
          }

          const text = extractTextFromMessage(message);

          if (!text) {
            ingestCounters.emptyText += 1;
            // Sample the message type keys so we know what's being dropped.
            // Useful to distinguish "all media, no captions" from "nothing".
            if (!key.fromMe) {
              lastEmptyTextSample = {
                at: new Date().toISOString(),
                remoteJid,
                messageKeys: Object.keys(message).slice(0, 6),
              };
            }
            continue;
          }
          const quotedText = extractQuotedTextFromMessage(message);
          const msgContextInfo = getContextInfo(message);
          const quotedMessageId = msgContextInfo?.stanzaId || null;
          const quotedSenderJid = msgContextInfo?.participant || null;

          const timestampMs = (messageTimestamp || Math.floor(Date.now() / 1000)) * 1000;

          if (key.fromMe) {
            // Debug-only trace to validate outbound/system messages in groups without forwarding them.
            if (logOwnMessages) {
              let ownGroupName = groupNameCache.get(remoteJid) || null;
              if (remoteJid.endsWith('@g.us') && !ownGroupName) {
                try {
                  const meta = await socket?.groupMetadata?.(remoteJid);
                  if (meta?.subject) {
                    ownGroupName = meta.subject;
                    groupNameCache.set(remoteJid, ownGroupName);
                  }
                } catch {
                  // Ignore metadata fetch errors for debug logging.
                }
              }

              console.log('[WhatsApp] Outgoing/own message detected (not forwarded):', {
                groupName: ownGroupName || '(unknown)',
                groupId: remoteJid,
                senderJid: key.participant || null,
                messageId: key.id || null,
                textPreview: text.slice(0, 120),
              });
            }
            continue; // Ignore own messages for webhook forwarding logic
          }

          // ✅ FIX: Create a unique key for deduplication
          // Use messageId if available, otherwise use text hash + timestamp
          const messageKey = key.id 
            ? `${remoteJid}_${key.id}` 
            : `${remoteJid}_${text.slice(0, 50)}_${Math.floor(timestampMs / 1000)}`;
          
          // Check if we've already processed this message in the last 30 seconds
          const lastProcessed = processedMessages.get(messageKey);
          const now = Date.now();
          if (lastProcessed && (now - lastProcessed) < 30000) {
            console.log('[WhatsApp] Skipping duplicate message:', messageKey);
            ingestCounters.duplicates += 1;
            continue;
          }
          
          // Mark as processed
          processedMessages.set(messageKey, now);
          
          // Clean up old entries (older than 1 minute)
          for (const [key, timestamp] of processedMessages.entries()) {
            if (now - timestamp > 60000) {
              processedMessages.delete(key);
            }
          }

          // Check if it's a group message or direct message
          if (remoteJid.endsWith('@g.us')) {
            // Group message - forward to /api/whatsapp/incoming
            // Obtain group name (cache to avoid repeated metadata calls)
            let groupName = groupNameCache.get(remoteJid) || null;
            if (!groupName) {
              try {
                const meta = await socket?.groupMetadata?.(remoteJid);
                if (meta?.subject) {
                  groupName = meta.subject;
                  groupNameCache.set(remoteJid, groupName);
                }
              } catch (err) {
                console.error('[WhatsApp] Error fetching group metadata for', remoteJid, err);
              }
            }

            if (logIncomingGroupMessages) {
              console.log('[WhatsApp] Incoming group message:', {
                groupName: groupName || '(unknown)',
                groupId: remoteJid,
                senderJid: key.participant || null,
                messageId: key.id || null,
                textPreview: text.slice(0, 120),
              });
            }

            const payload = {
              messageId: key.id || crypto.randomUUID(),
              groupId: remoteJid,
              groupName,
              senderJid: key.participant || null,
              senderName: pushName || null,
              text,
              quotedText,
              quotedMessageId,
              quotedSenderJid,
              timestamp: new Date(timestampMs).toISOString(),
              type,
            };

            await forwardIncomingMessage(payload);
            ingestCounters.forwarded += 1;
          } else {
            // Direct message - forward with a special groupId format
            // Use "direct_{senderJid}" as groupId to identify direct messages

            // Resolve the sender's phone JID when WhatsApp addresses this DM by
            // LID (privacy): remoteJidAlt carries the alternate (phone) form;
            // fall back to Baileys' LID→PN store so a `…@lid` sender still resolves
            // to a stable phone JID downstream.
            let senderJidAlt = key.remoteJidAlt || null;
            if (!senderJidAlt && remoteJid.endsWith('@lid')) {
              try {
                const pn = await socket?.signalRepository?.lidMapping?.getPNForLID?.(remoteJid);
                if (pn) senderJidAlt = pn;
              } catch (err) {
                console.error('[WhatsApp] LID→PN resolve failed (non-fatal):', err?.message || err);
              }
            }

            const directMessagePayload = {
              messageId: key.id || crypto.randomUUID(),
              groupId: `direct_${remoteJid}`, // Special groupId format for direct messages
              groupName: 'Direct Message',
              senderJid: remoteJid,
              senderJidAlt,
              senderName: pushName || null,
              text,
              quotedText,
              quotedMessageId,
              quotedSenderJid,
              timestamp: new Date(timestampMs).toISOString(),
              type,
            };

            // Use the same forwardIncomingMessage function (unified endpoint)
            await forwardIncomingMessage(directMessagePayload);
          }
        } catch (error) {
          console.error('[WhatsApp] Error handling incoming message:', error);
        }
      }
    });

    // Listen for group join requests (someone requests to join via invite link or linked group)
    // This is the primary event for join requests — group-participants.update does NOT emit 'request'.
    socket.ev.on('group.join-request', async ({ id, participant, participantPn, action, method }) => {
      try {
        if (action !== 'created') {
          console.log(`[WhatsApp] group.join-request action=${action} for ${participant} in ${id}, skipping (only 'created' triggers flow)`);
          return;
        }

        let groupName = groupNameCache.get(id) || null;
        if (!groupName) {
          try {
            const meta = await socket?.groupMetadata?.(id);
            if (meta?.subject) {
              groupName = meta.subject;
              groupNameCache.set(id, groupName);
            }
          } catch {
            // Ignore metadata fetch errors
          }
        }

        console.log(`[WhatsApp] group.join-request: participant=${participant}, phone=${participantPn || 'n/a'}, group=${groupName || id}, method=${method}`);

        // Cache LID↔phone mapping if we have both
        if (participant && participantPn) {
          joinRequestLidCache.set(`${id}:${participantPn}`, participant);
          joinRequestLidCache.set(`${id}:${participant}`, participantPn);
        }

        await forwardIncomingMessage({
          eventType: 'group_join_request',
          groupId: id,
          userJid: participant,
          ...(participantPn ? { phoneJid: participantPn } : {}),
          groupName,
          timestamp: new Date().toISOString(),
        });
      } catch (error) {
        console.error('[WhatsApp] Error handling group.join-request:', error);
      }
    });

    // Listen for group participant updates (adds, removes, promotions)
    socket.ev.on('group-participants.update', async ({ id, participants, action }) => {
      try {
        console.log(`[WhatsApp] Group participants update: group=${id}, action=${action}, participants=${participants?.length}`);

        // Forward new member adds to the flow engine (join requests handled by 'group.join-request')
        if (action === 'add') {
          let groupName = groupNameCache.get(id) || null;
          if (!groupName) {
            try {
              const meta = await socket?.groupMetadata?.(id);
              if (meta?.subject) {
                groupName = meta.subject;
                groupNameCache.set(id, groupName);
              }
            } catch {
              // Ignore metadata fetch errors
            }
          }

          for (const participant of (participants || [])) {
            const eventType = 'group_member_added';

            // Baileys emits participants as GroupParticipant objects (Contact & { ... })
            // with fields: id (preferred JID), lid (@lid), phoneNumber (@s.whatsapp.net).
            // Extract the LID as primary JID (used in flow executions) and phone as secondary.
            const isObject = typeof participant === 'object' && participant !== null;
            const participantId = isObject ? (participant.id || participant.lid || participant.phoneNumber) : participant;
            const participantLid = isObject ? participant.lid : undefined;
            const participantPhone = isObject ? participant.phoneNumber : undefined;

            // Use LID as preferred JID (consistent with how flow executions store userJid).
            // Fall back to id, then phone if no LID available.
            const userJid = participantLid || participantId || participantPhone;

            // If we have both LID and phone, cache the mapping for later resolution.
            if (participantLid && participantPhone) {
              joinRequestLidCache.set(`${id}:${participantPhone}`, participantLid);
              joinRequestLidCache.set(`${id}:${participantLid}`, participantPhone);
              console.log(`[WhatsApp] Cached LID↔phone mapping: ${participantLid} ↔ ${participantPhone} (group ${id})`);
            }

            // For 'add' events: if participant came as phone-only (string), try cached LID
            if (action === 'add' && typeof participant === 'string' && participant.endsWith('@s.whatsapp.net')) {
              const cachedLid = joinRequestLidCache.get(`${id}:${participant}`);
              if (cachedLid) {
                console.log(`[WhatsApp] Resolved phone JID ${participant} → LID ${cachedLid} via cache`);
                await forwardIncomingMessage({
                  eventType,
                  groupId: id,
                  userJid: cachedLid,
                  phoneJid: participant,
                  groupName,
                  timestamp: new Date().toISOString(),
                });
                joinRequestLidCache.delete(`${id}:${participant}`);
                joinRequestLidCache.delete(`${id}:${cachedLid}`);
                continue;
              }
            }

            console.log(`[WhatsApp] Forwarding ${eventType}: userJid=${userJid}, lid=${participantLid || 'n/a'}, phone=${participantPhone || 'n/a'}`);
            await forwardIncomingMessage({
              eventType,
              groupId: id,
              userJid,
              ...(participantPhone ? { phoneJid: participantPhone } : {}),
              groupName,
              timestamp: new Date().toISOString(),
            });
          }
        }
      } catch (error) {
        console.error('[WhatsApp] Error handling group participants update:', error);
      }
    });

    return socket;
  } catch (err) {
    console.error('[WhatsApp] Error initializing:', err);
    return null;
  } finally {
    // nos aseguramos de no quedarnos pegados
    if (connectionState !== 'open') {
      isReconnecting = false;
    }
  }
}

// Middleware para logging de requests
app.use((req, res, next) => {
  console.log(`[Service] ${req.method} ${req.path}`);
  next();
});

// Endpoints (health checks are already registered above, before express.json())

/**
 * GET /qr
 * Obtener QR code actual
 */
app.get('/qr', async (req, res) => {
  console.log('[API] GET /qr - Checking for QR code');
  if (!currentQRCode) {
    console.log('[API] No QR code available');
    return res.json({ success: false, message: 'No QR code available' });
  }

  console.log('[API] QR code found, returning to client');

  // `qrCode` is the raw Baileys payload and stays the contract for existing
  // callers. `qrImage` is the same payload rendered to a PNG data URL so a
  // browser can display it with a plain <img> and no client-side QR library.
  let qrImage = null;
  try {
    qrImage = await QRCode.toDataURL(currentQRCode, { margin: 1, width: 320 });
  } catch (error) {
    // A render failure must not withhold the raw payload: a caller that can
    // draw its own QR should still be able to.
    console.error('[API] QR render failed:', error?.message || error);
  }

  res.json({ success: true, qrCode: currentQRCode, qrImage });
});

/**
 * POST /clear-session
 * Wipe auth session and disconnect. After this, call /init to get a new QR.
 */
/**
 * POST /force-reconnect
 * Ends the current socket and re-initializes. Unlike /init, this does NOT
 * short-circuit when connectionState='open' — use it to escape a zombie
 * where Baileys reports connected but no inbound messages arrive.
 * The in-process watchdog uses the same primitive internally.
 */
app.post('/force-reconnect', async (req, res) => {
  console.log('[API] POST /force-reconnect - Forcing socket recreation');
  try {
    if (socket) {
      try { await socket.end(); } catch (e) { /* ignore */ }
    }
    socket = null;
    connectionState = 'close';
    isReconnecting = false;
    cancelScheduledReconnect();
    const sock = await initSocket();
    res.json({
      success: !!sock,
      connected: connectionState === 'open',
      connectionState,
    });
  } catch (error) {
    console.error('[API] Error in /force-reconnect:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/clear-session', async (req, res) => {
  console.log('[API] POST /clear-session - Wiping auth session');
  try {
    const result = await clearAuthSession();
    if (result) {
      res.json({ success: true, message: 'Session cleared. Call /init to generate new QR.' });
    } else {
      res.status(500).json({ success: false, error: 'Failed to clear session' });
    }
  } catch (error) {
    console.error('[API] Error clearing session:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /init
 * Inicializar conexión y generar QR
 */
app.post('/init', async (req, res) => {
  console.log('[API] POST /init - Request received');
  try {
    // Clear any existing QR code and reset flags
    currentQRCode = null;
    whatsappRefused = false; // Reset bloqueo para permitir reintento manual
    sessionInvalid401 = false; // Reset flag de sesión inválida
    
    // Si hay un socket existente pero está cerrado, limpiarlo primero
    if (socket && connectionState === 'close') {
      console.log('[API] Limpiando socket cerrado antes de inicializar...');
      try {
        await socket.end();
      } catch (e) {
        // Ignorar errores al cerrar
      }
      socket = null;
      isReconnecting = false;
      cancelScheduledReconnect();
    }
    
    // Si la sesión fue marcada como inválida, borrarla antes de inicializar
    if (sessionInvalid401) {
      console.log('[API] Sesión marcada como inválida, borrando antes de inicializar...');
      await clearAuthSession();
      sessionInvalid401 = false;
    }
    
    console.log('[API] Initializing socket...');
    const sock = await initSocket();
    if (!sock) {
      console.error('[API] Failed to initialize socket');
      return res.status(500).json({ success: false, message: 'Failed to initialize socket' });
    }

    console.log('[API] Socket initialized, connection state:', connectionState);
    console.log('[API] Waiting for QR code (up to 20 seconds)...');
    
    // Wait up to 20 seconds for QR code
    for (let i = 0; i < 20; i++) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      if (currentQRCode) {
        console.log('[API] QR code available, returning to client');
        return res.json({ success: true, qrGenerated: true, qrCode: currentQRCode });
      }
      if (connectionState === 'open') {
        console.log('[API] Already connected');
        return res.json({ success: true, connected: true, message: 'Already connected' });
      }
      // Check if blocked by 405 during wait
      if (whatsappRefused) {
        console.log('[API] WhatsApp blocked (405) detected during wait');
        return res.json({ 
          success: false, 
          message: 'WhatsApp está bloqueando la conexión (error 405). Por favor, espera 24-48 horas antes de intentar de nuevo.',
          whatsappBlocked: true,
          connectionState 
        });
      }
      
      // Check if session is invalid (401) during wait
      if (sessionInvalid401) {
        console.log('[API] Session invalid (401) detected during wait');
        // Borrar sesión y retornar mensaje apropiado
        await clearAuthSession();
        sessionInvalid401 = false;
        return res.json({ 
          success: false, 
          message: 'La sesión de WhatsApp es inválida. La sesión ha sido borrada. Por favor, intenta inicializar de nuevo para generar un nuevo QR code.',
          sessionInvalid: true,
          connectionState 
        });
      }
    }

    // Timeout reached - check if blocked by 405
    if (whatsappRefused) {
      console.log('[API] WhatsApp blocked (405) - timeout reached');
      return res.json({ 
        success: false, 
        message: 'WhatsApp está bloqueando la conexión (error 405). Por favor, espera 24-48 horas antes de intentar de nuevo.',
        whatsappBlocked: true,
        connectionState 
      });
    }
    
    // Timeout reached - check if session is invalid (401)
    if (sessionInvalid401) {
      console.log('[API] Session invalid (401) - timeout reached');
      await clearAuthSession();
      sessionInvalid401 = false;
      return res.json({ 
        success: false, 
        message: 'La sesión de WhatsApp es inválida. La sesión ha sido borrada. Por favor, intenta inicializar de nuevo para generar un nuevo QR code.',
        sessionInvalid: true,
        connectionState 
      });
    }

    // Timeout reached without QR or errors
    console.log('[API] QR not generated within timeout');
    console.log('[API] Connection state:', connectionState);
    res.json({ 
      success: false, 
      message: 'QR not generated within timeout. Connection state: ' + connectionState + '. Si la conexión falló con error 401, intenta inicializar de nuevo.',
      connectionState 
    });
  } catch (error) {
    console.error('[API] Error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * GET /status
 * Estado de la conexión
 */
app.get('/status', async (req, res) => {
  // Don't auto-initialize socket on status check
  // Only return current state
  const connected = connectionState === 'open' && !!socket?.user;

  res.json({
    success: true,
    connected,
    connectionState,
    hasQR: !!currentQRCode,
    phoneNumber: connected && socket?.user ? socket.user.id : null,
    socketInitialized: !!socket,
    whatsappBlocked: Boolean(whatsappRefused),
    refusedCode: whatsappRefused || null,
    lastDisconnect: lastDisconnectInfo,
  });
});

/**
 * GET /events
 * Server-Sent Events stream — pushes connection state changes to the browser in real time.
 * The Next.js route /api/whatsapp/events proxies this endpoint.
 */
app.get('/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.write(': connected\n\n');

  // Send current state immediately on connect
  const connected = connectionState === 'open' && !!socket?.user;
  res.write(`data: ${JSON.stringify({
    connected,
    connectionState,
    hasQR: !!currentQRCode,
    phoneNumber: connected ? socket?.user?.id : null,
    whatsappBlocked: Boolean(whatsappRefused),
    refusedCode: whatsappRefused || null,
    lastDisconnect: lastDisconnectInfo,
  })}\n\n`);

  const ping = setInterval(() => {
    try { res.write(': ping\n\n'); }
    catch { clearInterval(ping); sseClients.delete(res); }
  }, 25000);

  sseClients.add(res);
  req.on('close', () => { clearInterval(ping); sseClients.delete(res); });
});

/**
 * POST /cleanup-auth
 * Manually trigger cleanup of old Signal protocol key files
 */
app.post('/cleanup-auth', async (req, res) => {
  try {
    const filesBefore = await fs.readdir(AUTH_DIR).catch(() => []);
    await cleanupAuthKeyFiles();
    const filesAfter = await fs.readdir(AUTH_DIR).catch(() => []);

    // Build distribution of remaining files by prefix (first part before last dash+id)
    const distribution = {};
    for (const file of filesAfter) {
      // Extract prefix: everything up to the last segment (e.g., "sender-key-memory" from "sender-key-memory-123.json")
      const match = file.match(/^(.+?)-[^-]+\.json$/);
      const prefix = match ? match[1] : file;
      distribution[prefix] = (distribution[prefix] || 0) + 1;
    }

    res.json({
      success: true,
      filesBefore: filesBefore.length,
      filesAfter: filesAfter.length,
      deleted: filesBefore.length - filesAfter.length,
      distribution,
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /send
 * Enviar mensaje a grupo
 */
app.post('/send', async (req, res) => {
  try {
    const {
      groupId,
      message,
      imageUrl,
      // Optional: send as a WhatsApp "Reply" (quoted message)
      quotedMessageId,
      quotedText,
      quotedParticipant,
      quotedRemoteJid,
      // Optional: JIDs to mention in the message (triggers WhatsApp @ notification)
      mentions,
    } = req.body;
    
    if (!groupId || (!message && !imageUrl)) {
      return res.status(400).json({ success: false, error: 'groupId and (message or imageUrl) required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    const formattedGroupId = groupId.includes('@g.us') ? groupId : `${groupId}@g.us`;
    const quoted =
      quotedMessageId
        ? {
            key: {
              remoteJid: quotedRemoteJid || formattedGroupId,
              fromMe: false,
              id: quotedMessageId,
              ...(quotedParticipant ? { participant: quotedParticipant } : {}),
            },
            message: {
              conversation: typeof quotedText === 'string' && quotedText.length > 0 ? quotedText : ' ',
            },
          }
        : undefined;

    const content = imageUrl
      ? { image: { url: imageUrl }, caption: message || undefined, mentions: mentions || undefined }
      : { text: message, mentions: mentions || undefined };

    const sendResult = await sock.sendMessage(formattedGroupId, content, quoted ? { quoted } : undefined);
    const messageId = sendResult?.key?.id || null;
    console.log('[API] Group message sent:', { to: formattedGroupId, messageId });

    res.json({ success: true, to: formattedGroupId, messageId });
  } catch (error) {
    console.error('[API] Error sending message:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /send-direct
 * Enviar mensaje directo a un usuario
 * Opcionalmente puede enviar una imagen primero si imageUrl está presente
 */
app.post('/send-direct', async (req, res) => {
  try {
    const {
      senderJid,
      message,
      imageUrl,
      // Optional: send as a WhatsApp "Reply" (quoted message)
      quotedMessageId,
      quotedText,
      quotedParticipant,
      quotedRemoteJid,
    } = req.body;
    
    if (!senderJid) {
      return res.status(400).json({ success: false, error: 'senderJid is required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    const quoted =
      quotedMessageId
        ? {
            key: {
              remoteJid: quotedRemoteJid || senderJid,
              fromMe: false,
              id: quotedMessageId,
              ...(quotedParticipant ? { participant: quotedParticipant } : {}),
            },
            // Best-effort: include some text so the client renders the reply nicely.
            // For non-text quoted messages, this is still fine (it will show a generic preview).
            message: {
              conversation: typeof quotedText === 'string' && quotedText.length > 0 ? quotedText : ' ',
            },
          }
        : undefined;

    let lastMessageId = null;

    // Prefer sending ONE message (image + caption) to mimic link preview behavior.
    // Fallback to text-only if image fails.
    if (imageUrl) {
      try {
        const imgMsg = await sock.sendMessage(
          senderJid,
          { image: { url: imageUrl }, caption: message || undefined },
          quoted ? { quoted } : undefined
        );
        lastMessageId = imgMsg?.key?.id || lastMessageId;
        console.log('[API] Image message sent successfully', { to: senderJid, messageId: imgMsg?.key?.id || null });
        return res.json({ success: true, to: senderJid, messageId: lastMessageId });
      } catch (imageError) {
        console.warn('[API] Failed to send image message, falling back to text only:', imageError.message);
      }
    }

    // Send text message if provided (fallback)
    if (message) {
      const msgResult = await sock.sendMessage(senderJid, { text: message }, quoted ? { quoted } : undefined);
      lastMessageId = msgResult?.key?.id || lastMessageId;
      console.log('[API] Direct message sent:', { to: senderJid, messageId: msgResult?.key?.id || null });
    }

    // Resolve the recipient's LID (Linked ID) so callers can persist a
    // deterministic LID↔phone mapping. WhatsApp Multi-Device delivers
    // some inbound DMs with `@lid` JIDs that never match
    // Profile.phoneNumber, breaking inbound routing. Capturing the LID
    // here at outbound time gives us the bridge before any reply lands.
    // Best-effort: failure here MUST NOT fail the send.
    let lid = null;
    if (typeof senderJid === 'string' && senderJid.endsWith('@s.whatsapp.net')) {
      try {
        const phoneNumber = senderJid.replace('@s.whatsapp.net', '');
        const results = await sock.onWhatsApp(phoneNumber);
        const match = Array.isArray(results) ? results[0] : null;
        if (match?.lid && typeof match.lid === 'string') {
          lid = match.lid;
        }
      } catch (lidError) {
        console.warn('[API] LID resolution failed (non-fatal):', lidError?.message || lidError);
      }
    }

    res.json({ success: true, to: senderJid, messageId: lastMessageId, lid });
  } catch (error) {
    console.error('[API] Error sending direct message:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /revoke-message
 * Revoke ("delete for everyone") a previously sent message.
 * Requires the original message key: remoteJid + id. Only messages sent by this
 * WhatsApp account (fromMe=true) can be revoked, and only within WhatsApp's
 * revocation window (~48h for normal DMs, shorter for some chat types).
 *
 * Body:
 *   { remoteJid: string, messageId: string, participant?: string }
 *
 * Notes:
 *   - For direct messages, remoteJid is the counterparty JID (e.g. "...@lid" or "...@s.whatsapp.net").
 *   - For group messages, remoteJid is the group JID and `participant` is ignored for fromMe revocations.
 */
app.post('/revoke-message', async (req, res) => {
  try {
    const { remoteJid, messageId, participant } = req.body || {};
    if (!remoteJid || !messageId) {
      return res.status(400).json({ success: false, error: 'remoteJid and messageId are required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    const key = {
      remoteJid,
      fromMe: true,
      id: messageId,
      ...(participant ? { participant } : {}),
    };

    const result = await sock.sendMessage(remoteJid, { delete: key });
    const revocationId = result?.key?.id || null;
    console.log('[API] Message revoked:', { remoteJid, messageId, revocationId });

    res.json({ success: true, remoteJid, revokedMessageId: messageId, revocationId });
  } catch (error) {
    console.error('[API] Error revoking message:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /resolve-lid
 * Resolve a privacy LID JID ("...@lid") to its phone JID.
 * Tries Baileys' LID→PN signal store first; when `groupId` is provided,
 * falls back to that group's metadata (participants expose lid + phoneNumber).
 */
app.post('/resolve-lid', async (req, res) => {
  try {
    const { lid, groupId } = req.body || {};
    if (!lid || !String(lid).endsWith('@lid')) {
      return res.status(400).json({ success: false, error: 'lid ("...@lid") is required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    let phoneJid = null;
    let source = null;

    try {
      const pn = await sock.signalRepository?.lidMapping?.getPNForLID?.(lid);
      if (pn) {
        phoneJid = pn;
        source = 'lid-mapping';
      }
    } catch (err) {
      console.error('[API] resolve-lid: lidMapping lookup failed (non-fatal):', err?.message || err);
    }

    if (!phoneJid && groupId) {
      try {
        const meta = await sock.groupMetadata(groupId);
        const participant = (meta?.participants || []).find(
          (p) => p && (p.lid === lid || p.id === lid),
        );
        if (participant?.phoneNumber) {
          phoneJid = participant.phoneNumber;
          source = 'group-metadata';
        }
      } catch (err) {
        console.error('[API] resolve-lid: groupMetadata lookup failed (non-fatal):', err?.message || err);
      }
    }

    if (!phoneJid) {
      return res.status(404).json({ success: false, error: 'No phone mapping found for this LID', lid });
    }

    const phone = `+${String(phoneJid).split('@')[0].split(':')[0]}`;
    console.log('[API] LID resolved:', { lid, phoneJid, source });
    res.json({ success: true, lid, phoneJid, phone, source });
  } catch (error) {
    console.error('[API] Error resolving LID:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Extract invite code from WhatsApp group URL
 */
function extractInviteCode(groupUrl) {
  try {
    // Format: https://chat.whatsapp.com/CODIGO or https://chat.whatsapp.com/invite/CODIGO
    const url = new URL(groupUrl);
    const pathParts = url.pathname.split('/').filter(p => p);
    // Last part is usually the invite code
    return pathParts[pathParts.length - 1];
  } catch (error) {
    // If URL parsing fails, try to extract code directly
    const match = groupUrl.match(/(?:chat\.whatsapp\.com\/)(?:invite\/)?([A-Za-z0-9]+)/);
    return match ? match[1] : null;
  }
}

/**
 * POST /group/create
 * Create a group owned by the paired account.
 *
 * `participants` may be empty: WhatsApp allows a group of one, which is the
 * useful shape when the other people are meant to arrive through the invite
 * link rather than being added outright. The invite URL is resolved in the
 * same call because that is what every caller wants next, and a second
 * round-trip is another chance for the socket to drop in between.
 */
app.post('/group/create', async (req, res) => {
  try {
    const { subject, participants } = req.body;

    if (!subject || typeof subject !== 'string' || !subject.trim()) {
      return res.status(400).json({ success: false, error: 'subject is required' });
    }
    if (participants !== undefined && !Array.isArray(participants)) {
      return res.status(400).json({ success: false, error: 'participants must be an array of JIDs' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    const list = participants || [];
    console.log('[API] Creating group:', { subject, participants: list.length });

    const group = await sock.groupCreate(subject.trim(), list);
    const groupJid = group?.id;
    if (!groupJid) {
      return res.status(502).json({ success: false, error: 'Group created but no JID returned' });
    }

    // Which participants actually made it in. WhatsApp silently drops numbers
    // that block group adds, so the caller must be able to tell.
    const added = (group?.participants || []).map((p) => p.id || p.jid).filter(Boolean);

    let inviteUrl = null;
    let inviteCode = null;
    try {
      inviteCode = await sock.groupInviteCode(groupJid);
      inviteUrl = `https://chat.whatsapp.com/${inviteCode}`;
    } catch (error) {
      // The group exists either way; report it and let the caller retry the
      // link via /group/invite-url rather than losing the group entirely.
      console.error('[API] Group created but invite code failed:', error?.message || error);
    }

    res.json({
      success: true,
      groupJid,
      subject: group?.subject || subject.trim(),
      participants: added,
      inviteCode,
      inviteUrl,
    });
  } catch (error) {
    console.error('[API] Error creating group:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /group/invite-url
 * Current invite link for a group the paired account administers.
 *
 * Note this returns the group's *live* code: whoever holds it can join. Calling
 * WhatsApp's revoke is deliberately not exposed here — rotating a link people
 * were already given is a decision for a human, not a side effect of a lookup.
 */
app.post('/group/invite-url', async (req, res) => {
  try {
    const { groupId } = req.body;

    if (!groupId) {
      return res.status(400).json({ success: false, error: 'groupId is required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    const inviteCode = await sock.groupInviteCode(groupId);

    res.json({
      success: true,
      groupId,
      inviteCode,
      inviteUrl: `https://chat.whatsapp.com/${inviteCode}`,
    });
  } catch (error) {
    console.error('[API] Error getting invite url:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /group/join
 * Unirse a un grupo usando el código de invitación
 */
app.post('/group/join', async (req, res) => {
  try {
    const { groupUrl } = req.body;
    
    if (!groupUrl) {
      return res.status(400).json({ success: false, error: 'groupUrl is required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    const inviteCode = extractInviteCode(groupUrl);
    if (!inviteCode) {
      return res.status(400).json({ success: false, error: 'Invalid group URL format' });
    }

    console.log('[API] Joining group with invite code:', inviteCode);
    const groupJid = await sock.groupAcceptInvite(inviteCode);
    console.log('[API] Successfully joined group, JID:', groupJid);

    res.json({ 
      success: true, 
      groupId: groupJid,
      inviteCode 
    });
  } catch (error) {
    console.error('[API] Error joining group:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /group/get-jid
 * Obtener JID de un grupo si ya está en él
 */
app.post('/group/get-jid', async (req, res) => {
  try {
    const { groupUrl } = req.body;
    
    if (!groupUrl) {
      return res.status(400).json({ success: false, error: 'groupUrl is required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    const inviteCode = extractInviteCode(groupUrl);
    if (!inviteCode) {
      return res.status(400).json({ success: false, error: 'Invalid group URL format' });
    }

    console.log('[API] Getting group metadata for invite code:', inviteCode);
    
    // Get group metadata using the invite code
    // In Baileys, we can get group info from group metadata
    const groupMetadata = await sock.groupGetInviteInfo(inviteCode);
    
    if (!groupMetadata || !groupMetadata.id) {
      return res.status(404).json({ 
        success: false, 
        error: 'Group not found or you are not a member' 
      });
    }

    console.log('[API] Group metadata:', groupMetadata);
    const groupJid = groupMetadata.id;

    res.json({ 
      success: true, 
      groupId: groupJid,
      groupName: groupMetadata.subject || null,
      inviteCode 
    });
  } catch (error) {
    console.error('[API] Error getting group JID:', error);
    
    // If error indicates we're not in the group, return specific error
    if (error.message && (error.message.includes('not found') || error.message.includes('not a member'))) {
      return res.status(404).json({ 
        success: false, 
        error: 'Group not found or you are not a member. Try joining first.',
        needsJoin: true
      });
    }
    
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /group/add-participant
 * Add a user to a group. If it fails (e.g. privacy settings), get invite link and return it.
 */
app.post('/group/add-participant', async (req, res) => {
  try {
    const { groupId, userJid } = req.body;

    if (!groupId || !userJid) {
      return res.status(400).json({ success: false, error: 'groupId and userJid are required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    console.log('[API] Adding participant to group:', { groupId, userJid });

    // Try adding directly
    const result = await sock.groupParticipantsUpdate(groupId, [userJid], 'add');
    console.log('[API] groupParticipantsUpdate result:', JSON.stringify(result));

    // Check if participant was actually added
    const status = result?.[0]?.status || result?.[0]?.content?.attrs?.type;
    if (status === '403' || status === '409' || status === 'error') {
      // Could not add directly — get invite link as fallback
      console.log('[API] Direct add failed, getting invite link as fallback');
      const inviteCode = await sock.groupInviteCode(groupId);
      const inviteUrl = `https://chat.whatsapp.com/${inviteCode}`;
      return res.json({
        success: false,
        added: false,
        inviteUrl,
        reason: 'Could not add directly, invite link generated',
      });
    }

    res.json({ success: true, added: true });
  } catch (error) {
    console.error('[API] Error adding participant:', error);

    // Try to get invite link as last resort
    try {
      const sock2 = socket || await initSocket();
      if (sock2) {
        const inviteCode = await sock2.groupInviteCode(req.body.groupId);
        const inviteUrl = `https://chat.whatsapp.com/${inviteCode}`;
        return res.json({
          success: false,
          added: false,
          inviteUrl,
          reason: error.message,
        });
      }
    } catch (inviteError) {
      console.error('[API] Error getting invite link:', inviteError);
    }

    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /group/approve-join
 * Approve a pending group join request
 */
app.post('/group/approve-join', async (req, res) => {
  try {
    const { groupId, userJid } = req.body;

    if (!groupId || !userJid) {
      return res.status(400).json({ success: false, error: 'groupId and userJid are required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    console.log('[API] Approving join request:', { groupId, userJid });
    await sock.groupRequestParticipantsUpdate(groupId, [userJid], 'approve');

    res.json({ success: true });
  } catch (error) {
    console.error('[API] Error approving join request:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /group/reject-join
 * Reject a pending group join request
 */
app.post('/group/reject-join', async (req, res) => {
  try {
    const { groupId, userJid } = req.body;

    if (!groupId || !userJid) {
      return res.status(400).json({ success: false, error: 'groupId and userJid are required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    console.log('[API] Rejecting join request:', { groupId, userJid });
    await sock.groupRequestParticipantsUpdate(groupId, [userJid], 'reject');

    res.json({ success: true });
  } catch (error) {
    console.error('[API] Error rejecting join request:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /group/pending-requests
 * List pending join requests for a group
 */
app.post('/group/pending-requests', async (req, res) => {
  try {
    const { groupId } = req.body;

    if (!groupId) {
      return res.status(400).json({ success: false, error: 'groupId is required' });
    }

    const sock = socket || await initSocket();
    if (!sock || connectionState !== 'open') {
      return res.status(503).json({ success: false, error: 'WhatsApp not connected' });
    }

    console.log('[API] Listing pending join requests for group:', groupId);
    const pendingRequests = await sock.groupRequestParticipantsList(groupId);

    res.json({ success: true, pendingRequests });
  } catch (error) {
    console.error('[API] Error listing pending requests:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Auto-init on startup: after a short delay to let the HTTP server settle,
// initialize the Baileys socket automatically. Previously the service waited
// for an external POST /init, which meant any redeploy left the bot silent
// until somebody noticed and manually called /init. Gated by env var so the
// old behavior can be restored for debugging.
const AUTO_INIT_ON_STARTUP = process.env.WHATSAPP_AUTO_INIT !== '0';

// Watchdog: polls the liveness flag and forces a reconnect if we've been
// silent for too long during active hours. Addresses the zombie-socket class
// of failures where connectionState='open' but no messages arrive.
const WATCHDOG_CHECK_INTERVAL_MS = Number(process.env.WHATSAPP_WATCHDOG_INTERVAL_MS || 60_000);
const WATCHDOG_MAX_SILENCE_MS = Number(process.env.WHATSAPP_WATCHDOG_MAX_SILENCE_MS || 15 * 60 * 1000);
// Exponential backoff base; real cooldown doubles with each consecutive
// recovery, capped at MAX. Consecutive counter resets after QUIET_RESET_MS
// of no recoveries (i.e. the socket has been stable).
const WATCHDOG_RECOVERY_BASE_COOLDOWN_MS = Number(process.env.WHATSAPP_WATCHDOG_COOLDOWN_MS || 5 * 60 * 1000);
const WATCHDOG_RECOVERY_MAX_COOLDOWN_MS = Number(process.env.WHATSAPP_WATCHDOG_MAX_COOLDOWN_MS || 60 * 60 * 1000);
const WATCHDOG_QUIET_RESET_MS = Number(process.env.WHATSAPP_WATCHDOG_QUIET_RESET_MS || 2 * 60 * 60 * 1000);
// Active-hours window, so the watchdog stays quiet overnight when a silent
// socket is expected and any reconnect attempt would just be wasted churn
// (which also raises your risk profile with Meta). Hours are local to the
// timezone given by WHATSAPP_WATCHDOG_UTC_OFFSET (whole hours east of UTC;
// e.g. -5 for US Eastern, +1 for Central Europe). Defaults to UTC (offset 0),
// active 07:00–24:00. Set END to 24 for "through midnight".
const WATCHDOG_ACTIVE_HOUR_START = Number(process.env.WHATSAPP_WATCHDOG_ACTIVE_START || 7);
const WATCHDOG_ACTIVE_HOUR_END = Number(process.env.WHATSAPP_WATCHDOG_ACTIVE_END || 24);
const WATCHDOG_UTC_OFFSET_HOURS = Number(process.env.WHATSAPP_WATCHDOG_UTC_OFFSET || 0);

// Kill switch: set WHATSAPP_WATCHDOG_PAUSED=1 to skip every tick. Useful when a
// forced reconnect cycles the Baileys session so often that the paired phone
// spams sync notifications — pause the watchdog, leave the socket be, and
// recover manually when something actually breaks.
const WATCHDOG_PAUSED = /^(1|true|yes)$/i.test((process.env.WHATSAPP_WATCHDOG_PAUSED || '').trim());

// Exponential backoff state
let consecutiveRecoveries = 0;

function isWithinActiveHours () {
  const utcHour = new Date().getUTCHours();
  const localHour = (utcHour + WATCHDOG_UTC_OFFSET_HOURS + 24) % 24;
  const start = WATCHDOG_ACTIVE_HOUR_START;
  const end = WATCHDOG_ACTIVE_HOUR_END % 24; // 24 → 0 means "through midnight"
  if (WATCHDOG_ACTIVE_HOUR_END === 24 || end === 0) {
    // Active [start, 24) — i.e. from start to midnight
    return localHour >= start;
  }
  return start <= end
    ? localHour >= start && localHour < end
    : localHour >= start || localHour < end;
}

function currentRecoveryCooldownMs () {
  // Exponential: base * 2^n, capped. n=0 → base; n=1 → 2×base; n=2 → 4×base; ...
  const raw = WATCHDOG_RECOVERY_BASE_COOLDOWN_MS * Math.pow(2, consecutiveRecoveries);
  return Math.min(raw, WATCHDOG_RECOVERY_MAX_COOLDOWN_MS);
}

async function forceReconnect (reason) {
  consecutiveRecoveries += 1;
  const cooldownMin = Math.round(currentRecoveryCooldownMs() / 60000);
  console.warn(`[Watchdog] Forcing reconnect (consecutive=${consecutiveRecoveries}, next-cooldown=${cooldownMin}min): ${reason}`);
  lastWatchdogRecoveryAt = Date.now();
  try {
    if (socket) {
      try { await socket.end(); } catch (e) { /* ignore */ }
    }
  } finally {
    socket = null;
    connectionState = 'close';
    isReconnecting = false;
    // Drop any reconnect the ended socket's close handler may have scheduled, so
    // forceReconnect's own initSocket() is the single reconnect path.
    cancelScheduledReconnect();
    try {
      await initSocket();
    } catch (e) {
      console.error('[Watchdog] initSocket after forceReconnect failed:', e);
    }
  }
}

function startWatchdog () {
  if (WATCHDOG_PAUSED) {
    console.log('[Watchdog] Disabled via WHATSAPP_WATCHDOG_PAUSED — no automatic reconnects will be triggered.');
    return;
  }
  setInterval(() => {
    try {
      if (whatsappRefused || sessionInvalid401) return; // Nothing the watchdog can fix
      if (!isWithinActiveHours()) return;

      // Stay out of the way while a connection attempt is already in flight: a
      // pairing/handshake ('connecting'), an init in progress (isReconnecting), or
      // a reconnect already scheduled (reconnectTimer). Forcing a reconnect here is
      // exactly what spawned a competing socket and triggered the 440 conflict that
      // broke pairing. The QR-display state also reads 'connecting', so this keeps
      // the watchdog from nuking a socket that's just waiting to be scanned.
      if (isReconnecting || reconnectTimer || connectionState === 'connecting') return;

      const now = Date.now();

      // Reset the backoff counter if the socket has been stable for a while
      // (quiet = no forced recovery). Prevents the cooldown from staying at
      // 60 min forever after one bad night.
      if (consecutiveRecoveries > 0 && lastWatchdogRecoveryAt > 0
          && (now - lastWatchdogRecoveryAt) > WATCHDOG_QUIET_RESET_MS) {
        console.log(`[Watchdog] Resetting backoff after ${Math.round((now - lastWatchdogRecoveryAt) / 60000)} min of stability`);
        consecutiveRecoveries = 0;
      }

      if (now - lastWatchdogRecoveryAt < currentRecoveryCooldownMs()) return;

      // Case 1: socket claims 'open' but no messages in a long time → zombie.
      if (connectionState === 'open' && lastMessageReceivedAt
          && (now - lastMessageReceivedAt) > WATCHDOG_MAX_SILENCE_MS) {
        const mins = Math.round((now - lastMessageReceivedAt) / 60000);
        forceReconnect(`zombie socket: open but silent for ${mins} min`);
        return;
      }

      // Case 2: socket not initialized at all during active hours → init it.
      if (!socket || connectionState === 'close') {
        forceReconnect(`socket not initialized (state=${connectionState})`);
        return;
      }
    } catch (e) {
      console.error('[Watchdog] tick error:', e);
    }
  }, WATCHDOG_CHECK_INTERVAL_MS);
  console.log(`[Watchdog] Started: check every ${WATCHDOG_CHECK_INTERVAL_MS / 1000}s, max silence ${WATCHDOG_MAX_SILENCE_MS / 60000} min, active ${WATCHDOG_ACTIVE_HOUR_START}h–${WATCHDOG_ACTIVE_HOUR_END}h (UTC${WATCHDOG_UTC_OFFSET_HOURS >= 0 ? '+' : ''}${WATCHDOG_UTC_OFFSET_HOURS}), backoff base ${WATCHDOG_RECOVERY_BASE_COOLDOWN_MS/60000}min → cap ${WATCHDOG_RECOVERY_MAX_COOLDOWN_MS/60000}min`);
}

console.log('[Service] WhatsApp service started.');
console.log(`[Service] Starting server on port ${PORT}...`);

// Bind 0.0.0.0 and honor the platform-assigned PORT — required by most PaaS
// hosts (Railway, Render, Fly.io, …) to route traffic to the container.
const server = app.listen(PORT, '0.0.0.0', () => {
  const address = server.address();
  console.log(`[Service] WhatsApp service listening on port ${PORT}`);
  console.log(`[Service] Server address: ${JSON.stringify(address)}`);
  console.log(`[Service] Auth directory: ${AUTH_DIR}`);
  console.log(`[Service] Health check available at: http://0.0.0.0:${PORT}/`);
  console.log(`[Service] Server is ready to accept connections`);

  // Start periodic cleanup of old Signal protocol key files
  startAuthCleanupSchedule();

  // Start watchdog regardless of auto-init setting — it also covers the
  // case where auto-init is disabled and the operator forgets to call /init.
  startWatchdog();


  // Immediately test that server is responding
  setTimeout(() => {
    const testReq = http.get(`http://0.0.0.0:${PORT}/`, (res) => {
      console.log(`[Service] Self-test: Server responded with status ${res.statusCode}`);
    });
    testReq.on('error', (err) => {
      console.error(`[Service] Self-test failed: ${err.message}`);
    });
  }, 100);

  // Auto-initialize the WhatsApp socket on startup. Delayed 2s so the HTTP
  // server is already responding to health checks before Baileys starts
  // doing any network work.
  if (AUTO_INIT_ON_STARTUP) {
    setTimeout(async () => {
      try {
        console.log('[Service] Auto-initializing WhatsApp socket...');
        await initSocket();
      } catch (e) {
        console.error('[Service] Auto-init failed:', e);
      }
    }, 2000);
  } else {
    console.log('[Service] WHATSAPP_AUTO_INIT=0 — skipping auto-init; call POST /init manually.');
  }
});

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  console.log('[Service] SIGTERM received, shutting down gracefully...');

  if (authCleanupTimer) clearInterval(authCleanupTimer);

  try {
    if (socket) {
      console.log('[Service] Closing WhatsApp socket...');
      try {
        await socket.end();
      } catch (e) {
        console.error('[Service] Error closing WhatsApp socket:', e);
      }
    }
  } finally {
    server.close(() => {
      console.log('[Service] Server closed');
      process.exit(0);
    });
  }
});
