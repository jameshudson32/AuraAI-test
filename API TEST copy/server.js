// server.js (fixed)

const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const axios = require('axios');
const path = require('path');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: process.env.CORS_ORIGIN || '*',
    methods: ['GET', 'POST'],
  },
});

// =========================
/**
 * Configuration
 */
const OF_API_BASE = process.env.OF_API_BASE || 'https://app.onlyfansapi.com';
const OF_API_KEY = process.env.OF_API_KEY;
const OF_ACCOUNT_ID = process.env.OF_ACCOUNT_ID;
const OF_SCRAPE_ON_LIST = process.env.OF_SCRAPE_ON_LIST !== 'false'; // default true
const OF_SCRAPE_EXPIRES_IN = parseInt(process.env.OF_SCRAPE_EXPIRES_IN || '604800', 10); // 7 days
const OF_SCRAPE_CONCURRENCY = parseInt(process.env.OF_SCRAPE_CONCURRENCY || '5', 10);

// =========================
// Middleware / Static
// =========================
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// =========================
/** In-memory scrape cache: Map<cdnUrl, { scrapedUrl: string, expiresAt: Date }> */
const scrapeCache = new Map();

// =========================
// Simple concurrency limiter
// =========================
class ConcurrencyLimiter {
  constructor(maxConcurrent) {
    this.maxConcurrent = maxConcurrent;
    this.running = 0;
    this.queue = [];
  }
  async run(fn) {
    while (this.running >= this.maxConcurrent) {
      await new Promise((resolve) => this.queue.push(resolve));
    }
    this.running++;
    try {
      return await fn();
    } finally {
      this.running--;
      const next = this.queue.shift();
      if (next) next();
    }
  }
}
const scrapeLimiter = new ConcurrencyLimiter(OF_SCRAPE_CONCURRENCY);

// =========================
// Axios client (120s timeout)
// =========================
const apiClient = axios.create({
  baseURL: OF_API_BASE,
  timeout: 120000, // 120s to avoid mid-transfer timeouts on large files
  headers: {
    Authorization: `Bearer ${OF_API_KEY}`,
    'Content-Type': 'application/json',
  },
});

// =========================
// Helpers
// =========================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Decide if a URL looks like a transformed/preview asset rather than the original.
 * We avoid scraping these because they frequently 403/422.
 */
function isTransformedUrl(url) {
  if (!url) return true;
  const lowered = url.toLowerCase();
  // common transforms / thumbs / frames / dimension-prefixed
  const badMarkers = [
    '960x960_',          // resized square preview
    '_preview',          // preview markers
    '_thumb',            // thumbnail markers
    'frame_0.jpg',       // video frame grabs
    '/thumbs/',          // thumbs path
    '/previews/',        // previews path
  ];
  if (badMarkers.some((m) => lowered.includes(m))) return true;

  // dimension-prefixed like 464x848_..., 600x400_..., etc.
  // also allow cdn query params, so just check the path segment
  const pathPart = lowered.split('?')[0];
  const fileName = pathPart.split('/').pop();
  if (/^\d{2,4}x\d{2,4}_/.test(fileName)) return true;

  return false;
}

/**
 * Extract best original URL from a media item.
 * Priority: files.full.url (esp. videos) -> files.source.url (images) -> legacy source
 * Explicitly skip transformed/preview/thumbnail URLs.
 */
function extractOriginalUrl(item) {
  const type = item.type || 'photo';
  const id = item.id;

  const candidates = [
    item.files?.full?.url,     // videos: usually *_source.mp4
    item.files?.source?.url,   // images: original
    item.source?.source,       // legacy
    typeof item.source === 'string' ? item.source : null,
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (!isTransformedUrl(candidate)) {
      console.log(`[EXTRACT-URL] Using original for item ${id} (${type}) -> ${candidate.substring(0, 80)}...`);
      return candidate;
    } else {
      console.log(`[EXTRACT-URL] Skipped transformed candidate for item ${id}: ${candidate.substring(0, 80)}...`);
    }
  }

  console.log(`[EXTRACT-URL] No acceptable original URL for item ${id} (${type}).`);
  return null;
}

/**
 * Retry wrapper: retries on network timeouts and 5xx with exponential backoff.
 */
async function withRetries(fn, { attempts = 3, baseDelayMs = 500 } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const status = err.response?.status;
      const isTimeout = err.code === 'ECONNABORTED';
      const is5xx = status >= 500 && status < 600;

      if (!isTimeout && !is5xx) break; // don't retry non-timeout/non-5xx

      const delay = baseDelayMs * Math.pow(2, i);
      console.warn(`[RETRY] Attempt ${i + 1} failed (${isTimeout ? 'timeout' : status}). Retrying in ${delay}ms...`);
      await sleep(delay);
    }
  }
  throw lastErr;
}

/**
 * Scrape a single CDN URL via API, caching result.
 * Sends both url and cdn_url to satisfy either request schema.
 */
async function scrapeUrl(cdnUrl, accountId) {
  // cache hit?
  const cached = scrapeCache.get(cdnUrl);
  if (cached && cached.expiresAt > new Date()) {
    console.log(`[SCRAPE] Cache hit (expires ${cached.expiresAt.toISOString()})`);
    return { url: cached.scrapedUrl };
  }
  if (cached) {
    console.log(`[SCRAPE] Cache expired, removing`);
    scrapeCache.delete(cdnUrl);
  }

  return scrapeLimiter.run(async () => {
    try {
      console.log(`[SCRAPE] -> ${cdnUrl.substring(0, 120)}...`);
      const body = {
        url: cdnUrl,            // some deployments require 'url'
        cdn_url: cdnUrl,        // others expect 'cdn_url'
        expires_in: OF_SCRAPE_EXPIRES_IN,
      };

      const response = await withRetries(
        () => apiClient.post(`/api/${accountId}/media/scrape`, body),
        { attempts: 3, baseDelayMs: 700 }
      );

      const data = response.data || {};
      // field possibilities: temporary_url (preferred), url, scraped_url, data.url, data.scraped_url
      const scrapedUrl =
        data.temporary_url ||
        data.url ||
        data.scraped_url ||
        data?.data?.url ||
        data?.data?.scraped_url ||
        data.scrapedUrl;

      if (!scrapedUrl) {
        console.error(`[SCRAPE] No URL in response. Keys: ${Object.keys(data || {}).join(', ')}`);
        return { errorCode: 'scrape_failed_unknown', message: 'No scraped URL returned' };
      }

      const expiresAt = new Date(Date.now() + OF_SCRAPE_EXPIRES_IN * 1000);
      scrapeCache.set(cdnUrl, { scrapedUrl, expiresAt });

      console.log(`[SCRAPE] OK (cached until ${expiresAt.toISOString()})`);
      return { url: scrapedUrl };
    } catch (error) {
      const status = error.response?.status;
      const msg = error.response?.data?.message || error.response?.data?.error || error.message;

      if (status === 403) {
        console.error(`[SCRAPE] 403 restricted: ${msg}`);
        return { errorCode: 'restricted', message: msg || 'Access restricted' };
      }
      if (error.code === 'ECONNABORTED') {
        console.error(`[SCRAPE] timeout: ${msg}`);
        return { errorCode: 'scrape_failed_timeout', message: 'Scrape timed out' };
      }
      if (status >= 500) {
        console.error(`[SCRAPE] 5xx: ${status} ${msg}`);
        return { errorCode: 'scrape_failed_5xx', message: msg || 'Server error during scrape' };
      }
      if (status >= 400) {
        console.error(`[SCRAPE] 4xx: ${status} ${msg}`);
        return { errorCode: 'scrape_failed_4xx', message: msg || 'Bad request during scrape' };
      }
      console.error(`[SCRAPE] unknown error: ${msg}`);
      return { errorCode: 'scrape_failed_unknown', message: msg || 'Unknown scrape error' };
    }
  });
}

/**
 * Normalize a media item for the frontend.
 * (CHANGED: allow previews/resizes as fallback when no original is available)
 */
async function normalizeMediaItem(item, scrapeEnabled = true, accountId) {
  const normalized = {
    id: item.id,
    type: item.type || 'photo',
    createdAt: item.createdAt || item.created_at || new Date().toISOString(),
    canView: item.canView !== false,
    locked: item.canView === false,
  };

  console.log(`[NORMALIZE] Item ${item.id} (${normalized.type})`);

  // Get original URL only (skip previews/thumbs)
  const originalUrl = extractOriginalUrl(item);

  // --- NEW: if no original, fall back to any preview/resize for thumbnails/posters ---
  if (!originalUrl) {
    const previewCandidates = [
      item.files?.preview?.url,
      item.files?.thumb?.url,
      item.preview?.url,
      item.thumb?.url,
      item.preview,
      item.thumb,
      // sometimes preview/resize lives directly under files.source for photos
      item.files?.source?.url, // allow even if transformed; we're using it only as a preview
    ].filter(Boolean);

    if (previewCandidates.length) {
      const preview = previewCandidates[0];

      if (normalized.type === 'video') {
        // Poster only; no playback/download without a scraped original
        normalized.previewUrl = preview;
        normalized.previewOnly = true;
        return normalized;
      } else {
        // Photo thumbnail fallback
        normalized.previewUrl = preview;
        normalized.imageUrl = preview;
        normalized.previewOnly = true;
        return normalized;
      }
    }

    // If absolutely nothing exists, keep prior error behavior
    normalized.error = true;
    normalized.errorCode = 'no_source_url';
    normalized.errorMessage = 'No original URL available';
    return normalized;
  }
  // --- END NEW ---

  // Scrape
  if (!scrapeEnabled) {
    normalized.error = true;
    normalized.errorCode = 'scraping_disabled';
    normalized.errorMessage = 'Scraping disabled';
    return normalized;
  }

  const result = await scrapeUrl(originalUrl, accountId);
  if (!result || (!result.url && !result.errorCode)) {
    normalized.error = true;
    normalized.errorCode = 'scrape_failed_unknown';
    normalized.errorMessage = 'Scrape failed';
    return normalized;
  }
  if (result.errorCode) {
    normalized.error = true;
    normalized.errorCode = result.errorCode;
    normalized.errorMessage = result.message || 'Scrape failed';
    return normalized;
  }

  const scrapedUrl = result.url;

  // Assign URLs
  if (normalized.type === 'video') {
    normalized.previewUrl = scrapedUrl; // if you prefer a poster, you can later add thumb scraping here
    normalized.playbackUrl = scrapedUrl;
    normalized.downloadUrl = scrapedUrl;
  } else {
    normalized.previewUrl = scrapedUrl;
    normalized.imageUrl = scrapedUrl;
    normalized.downloadUrl = scrapedUrl;
  }

  return normalized;
}

// =========================
// Routes
// =========================

// Connected accounts
app.get('/api/accounts', async (req, res) => {
  console.log('[ACCOUNTS] Fetching connected accounts');
  try {
    let accounts = [];

    if (OF_ACCOUNT_ID) {
      try {
        const accountResponse = await apiClient.get(`/api/${OF_ACCOUNT_ID}/me`);
        const accountData = accountResponse.data?.data || accountResponse.data || {};
        accounts.push({
          ...accountData,
          id: OF_ACCOUNT_ID, // keep acct_ id for routing
          numericId: accountData.id,
          username: accountData.username || accountData.name || OF_ACCOUNT_ID,
          name: accountData.name || accountData.username || OF_ACCOUNT_ID,
        });
        console.log(`[ACCOUNTS] Loaded account: ${OF_ACCOUNT_ID} (numeric: ${accountData.id})`);
      } catch (e) {
        console.error(`[ACCOUNTS] Failed to load details for ${OF_ACCOUNT_ID}: ${e.message}`);
        accounts.push({ id: OF_ACCOUNT_ID, username: OF_ACCOUNT_ID, name: OF_ACCOUNT_ID });
      }
    }

    if (accounts.length === 0) {
      try {
        const whoamiResponse = await apiClient.get('/whoami');
        const data = whoamiResponse.data || {};
        if (data.accounts) accounts = data.accounts;
        else if (data.data?.accounts) accounts = data.data.accounts;
        else if (data.connectedAccounts) accounts = data.connectedAccounts;
        else if (data.account) accounts = [data.account];
        console.log(`[ACCOUNTS] Found ${accounts.length} accounts via whoami`);
      } catch (e) {
        console.log('[ACCOUNTS] whoami not available:', e.message);
      }
    }

    console.log(`[ACCOUNTS] Returning ${accounts.length} accounts`);
    res.json(accounts);
  } catch (error) {
    console.error('[ACCOUNTS] Error:', error.message);
    res.status(500).json({ error: 'Failed to fetch accounts' });
  }
});

// Vault media with pagination + scrape
app.get('/api/accounts/:accountId/media', async (req, res) => {
  try {
    const { accountId } = req.params;
    const { limit = 25, offset = 0, order = 'publish_date_desc' } = req.query;

    console.log(`[VAULT] Fetching media for ${accountId} | limit=${limit} offset=${offset}`);

    const fetchLimit = Math.min(parseInt(limit) + parseInt(offset) + 1, 100);
    const vaultResponse = await apiClient.get(`/api/${accountId}/media/vault`, {
      params: { limit: fetchLimit, order },
    });

    let items = [];
    if (vaultResponse.data?.data?.list) items = vaultResponse.data.data.list;
    else if (Array.isArray(vaultResponse.data?.data)) items = vaultResponse.data.data;
    else if (Array.isArray(vaultResponse.data)) items = vaultResponse.data;
    else if (vaultResponse.data?.list) items = vaultResponse.data.list;

    if (!Array.isArray(items)) {
      console.warn('[VAULT] Unexpected response shape:', Object.keys(vaultResponse.data || {}));
      items = [];
    }

    console.log(`[VAULT] Total from API: ${items.length}`);

    const startIndex = parseInt(offset);
    const endIndex = startIndex + parseInt(limit);
    const paginatedItems = items.slice(startIndex, endIndex);
    const hasMore = items.length > endIndex;

    console.log(`[VAULT] Normalizing ${paginatedItems.length} items (scrape=${OF_SCRAPE_ON_LIST})`);

    const normalizedItems = await Promise.all(
      paginatedItems.map((item) => normalizeMediaItem(item, OF_SCRAPE_ON_LIST, accountId))
    );

    const successCount = normalizedItems.filter((i) => !i.error).length;
    const errorCount = normalizedItems.filter((i) => i.error).length;
    console.log(`[VAULT] Done. Success=${successCount} Errors=${errorCount} Cache=${scrapeCache.size}`);

    res.json({
      media: normalizedItems,
      total: items.length,
      hasMore,
      offset: startIndex,
      limit: parseInt(limit),
    });
  } catch (error) {
    console.error('[VAULT] Error:', error.message);
    if (error.response?.data) console.error('[VAULT] API response:', error.response.data);
    res.status(500).json({ error: 'Failed to fetch media', message: error.message });
  }
});

// Messages (best-effort)
app.get('/api/accounts/:accountId/messages', async (req, res) => {
  console.log('[MESSAGES] Fetching recent messages');
  try {
    const { accountId } = req.params;
    const messages = [];

    try {
      const chatsResponse = await apiClient.get(`/api/${accountId}/chats`);
      const chats = Array.isArray(chatsResponse.data)
        ? chatsResponse.data
        : chatsResponse.data?.data || chatsResponse.data?.list || [];

      console.log(`[MESSAGES] Found ${chats.length} chats`);

      for (let i = 0; i < Math.min(5, chats.length); i++) {
        const chat = chats[i];
        const chatId = chat.id || chat.chatId || chat.fan?.id || chat.withUser?.id;
        if (!chatId) continue;

        try {
          const msgResponse = await apiClient.get(`/api/${accountId}/chats/${chatId}/messages?limit=10`);
          const chatMessages = Array.isArray(msgResponse.data)
            ? msgResponse.data
            : msgResponse.data?.data || msgResponse.data?.list || [];

          if (!Array.isArray(chatMessages)) continue;

          messages.push(
            ...chatMessages.map((msg) => ({
              id: msg.id,
              text: msg.text || msg.message || '',
              createdAt: msg.createdAt || msg.created_at || new Date().toISOString(),
              fromUser: {
                name: chat.withUser?.name || chat.fan?.name || 'User',
                id: chat.withUser?.id || chat.fan?.id,
              },
              chatId,
            }))
          );
        } catch (e) {
          console.error(`[MESSAGES] Failed to load messages for chat ${chatId}:`, e.message);
        }
      }
    } catch (e) {
      console.error('[MESSAGES] Failed to load chats:', e.message);
    }

    messages.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    console.log(`[MESSAGES] Returning ${messages.length} messages`);
    res.json(messages.slice(0, 50));
  } catch (error) {
    console.error('[MESSAGES] Error:', error.message);
    res.json([]);
  }
});

// Activity (best-effort)
app.get('/api/accounts/:accountId/activity', async (req, res) => {
  console.log('[ACTIVITY] Fetching recent activity');
  try {
    const { accountId } = req.params;
    const activities = [];

    // subscribers
    try {
      const subsResponse = await apiClient.get(`/api/${accountId}/subscribers/active?limit=10`);
      const subs = Array.isArray(subsResponse.data)
        ? subsResponse.data
        : subsResponse.data?.data || subsResponse.data?.list || [];

      subs.forEach((sub) => {
        activities.push({
          type: 'subscription',
          description: `New subscriber: ${sub.name || sub.username || 'Anonymous'}`,
          amount: sub.currentSubscribePrice || sub.subscribePrice || 0,
          timestamp: sub.subscribedOnData?.subscribedAt || sub.subscribedAt || new Date().toISOString(),
        });
      });
      console.log(`[ACTIVITY] Subscribers: ${subs.length}`);
    } catch (e) {
      console.error('[ACTIVITY] Subscribers error:', e.message);
    }

    // transactions
    try {
      const transResponse = await apiClient.get(`/api/${accountId}/payouts/transactions?limit=20`);
      const transactions = Array.isArray(transResponse.data)
        ? transResponse.data
        : transResponse.data?.data || transResponse.data?.list || [];

      if (Array.isArray(transactions)) {
        transactions.forEach((tx) => {
          let type = 'purchase';
          let description = 'Purchase';
          if (tx.description?.toLowerCase().includes('tip') || tx.type === 'tip') {
            type = 'tip';
            description = `Tip from ${tx.user?.name || 'Anonymous'}`;
          } else if (tx.description?.toLowerCase().includes('message')) {
            description = `Message purchase from ${tx.user?.name || 'Anonymous'}`;
          }
          activities.push({
            type,
            description,
            amount: tx.amount || tx.price || 0,
            timestamp: tx.createdAt || tx.created_at || new Date().toISOString(),
          });
        });
      }
      console.log(`[ACTIVITY] Transactions: ${Array.isArray(transactions) ? transactions.length : 0}`);
    } catch (e) {
      console.error('[ACTIVITY] Transactions error:', e.message);
    }

    activities.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    console.log(`[ACTIVITY] Returning ${activities.length} activities`);
    res.json(activities.slice(0, 20));
  } catch (error) {
    console.error('[ACTIVITY] Error:', error.message);
    res.json([]);
  }
});

// Stats
app.get('/api/accounts/:accountId/stats', async (req, res) => {
  console.log('[STATS] Fetching account statistics');
  try {
    const { accountId } = req.params;
    let stats = {
      subscribers: 0,
      revenueToday: '0.00',
      postsCount: 0,
      photosCount: 0,
      videosCount: 0,
    };

    try {
      const accountResponse = await apiClient.get(`/api/${accountId}/me`);
      const accountData = accountResponse.data?.data || accountResponse.data || {};
      stats.subscribers = accountData.subscribersCount || accountData.counters?.subscribersCount || 0;
      stats.postsCount = accountData.postsCount || accountData.counters?.postsCount || 0;
      stats.photosCount = accountData.photosCount || accountData.counters?.photosCount || 0;
      stats.videosCount = accountData.videosCount || accountData.counters?.videosCount || 0;
    } catch (e) {
      console.error('[STATS] Account data error:', e.message);
    }

    try {
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const tomorrow = new Date(today);
      tomorrow.setDate(tomorrow.getDate() + 1);

      const transResponse = await apiClient.get(
        `/api/${accountId}/payouts/transactions?startDate=${today.toISOString()}&endDate=${tomorrow.toISOString()}`
      );
      const transactions = Array.isArray(transResponse.data)
        ? transResponse.data
        : transResponse.data?.data || transResponse.data?.list || [];
      if (Array.isArray(transactions)) {
        const todayRevenue = transactions.reduce((sum, tx) => sum + (tx.amount || tx.price || 0), 0);
        stats.revenueToday = todayRevenue.toFixed(2);
        console.log(`[STATS] Today's revenue: $${stats.revenueToday}`);
      }
    } catch (e) {
      console.error("[STATS] Today's revenue error:", e.message);
    }

    res.json(stats);
  } catch (error) {
    console.error('[STATS] Error:', error.message);
    res.json({
      subscribers: 0,
      revenueToday: '0.00',
      postsCount: 0,
      photosCount: 0,
      videosCount: 0,
    });
  }
});

// On-demand single scrape
app.post('/api/accounts/:accountId/media/scrape-single', async (req, res) => {
  try {
    const { accountId } = req.params;
    const { cdn_url } = req.body;
    if (!cdn_url) return res.status(400).json({ error: 'cdn_url required' });

    if (isTransformedUrl(cdn_url)) {
      return res.status(422).json({ error: 'not_original_url', message: 'Provide an original CDN URL, not a preview/thumb.' });
    }

    const result = await scrapeUrl(cdn_url, accountId);
    if (result.errorCode) return res.status(502).json(result);
    res.json({ url: result.url });
  } catch (error) {
    console.error('[SCRAPE-SINGLE] Error:', error.message);
    res.status(500).json({ error: 'Failed to scrape URL', message: error.message });
  }
});

// Get scraped URL for a specific media item
app.get('/api/accounts/:accountId/media/scraped-url', async (req, res) => {
  try {
    const { accountId } = req.params;
    const { id } = req.query;
    if (!id) return res.status(400).json({ error: 'Media ID required' });

    console.log(`[SCRAPED-URL] Item ${id}`);

    const vaultResponse = await apiClient.get(`/api/${accountId}/media/vault?limit=100`);
    let items = [];
    if (vaultResponse.data?.data?.list) items = vaultResponse.data.data.list;
    else if (Array.isArray(vaultResponse.data?.data)) items = vaultResponse.data.data;
    else if (Array.isArray(vaultResponse.data)) items = vaultResponse.data;

    const item = items.find((m) => String(m.id) === String(id));
    if (!item) return res.status(404).json({ error: 'Media not found' });

    const cdnUrl = extractOriginalUrl(item);
    if (!cdnUrl) return res.status(404).json({ error: 'No source URL available' });

    const result = await scrapeUrl(cdnUrl, accountId);
    if (result.errorCode) return res.status(502).json(result);

    res.json({
      url: result.url,
      type: item.type || 'photo',
      canView: item.canView !== false,
    });
  } catch (error) {
    console.error('[SCRAPED-URL] Error:', error.message);
    res.status(500).json({ error: 'Failed to get scraped URL' });
  }
});

// Health
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    config: {
      apiBase: OF_API_BASE,
      accountId: OF_ACCOUNT_ID,
      apiKeySet: !!OF_API_KEY,
      scrapeOnList: OF_SCRAPE_ON_LIST,
      scrapeExpiresIn: OF_SCRAPE_EXPIRES_IN,
      scrapeConcurrency: OF_SCRAPE_CONCURRENCY,
      cacheSize: scrapeCache.size,
      timeoutMs: apiClient.defaults.timeout,
    },
  });
});

// =========================
// WebSocket
// =========================
io.on('connection', (socket) => {
  console.log('[SOCKET] Client connected');

  socket.on('join-account', (accountId) => {
    socket.join(`account-${accountId}`);
    console.log(`[SOCKET] Client joined room: account-${accountId}`);
  });

  socket.on('disconnect', () => {
    console.log('[SOCKET] Client disconnected');
  });
});

// =========================
// Start server
// =========================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`[SERVER] Started on port ${PORT}`);
  console.log(`[CONFIG] API Base: ${OF_API_BASE}`);
  console.log(`[CONFIG] Account ID: ${OF_ACCOUNT_ID}`);
  console.log(`[CONFIG] API Key: ${OF_API_KEY ? 'Set' : 'Not set'}`);
  console.log(`[CONFIG] Scrape on list: ${OF_SCRAPE_ON_LIST}`);
  console.log(`[CONFIG] Scrape expires in: ${OF_SCRAPE_EXPIRES_IN}s`);
  console.log(`[CONFIG] Scrape concurrency: ${OF_SCRAPE_CONCURRENCY}`);
});
