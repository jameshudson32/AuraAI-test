// server.js (Enhanced with PostgreSQL)

const express = require('express');
const cors = require('cors');
const http = require('http');
const socketIo = require('socket.io');
const axios = require('axios');
const path = require('path');
const { URL } = require('url');
const { Pool } = require('pg');
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
// Configuration
// =========================
const OF_API_BASE = process.env.OF_API_BASE || 'https://app.onlyfansapi.com';
const OF_API_KEY = process.env.OF_API_KEY;
const OF_ACCOUNT_ID = process.env.OF_ACCOUNT_ID;
const OF_SCRAPE_ON_LIST = process.env.OF_SCRAPE_ON_LIST !== 'false';
const OF_SCRAPE_EXPIRES_IN = parseInt(process.env.OF_SCRAPE_EXPIRES_IN || '3124224000', 10); // 99 years
const OF_SCRAPE_CONCURRENCY = parseInt(process.env.OF_SCRAPE_CONCURRENCY || '3', 10);
const OF_SCRAPE_TTL_MINUTES = process.env.OF_SCRAPE_TTL_MINUTES ? parseInt(process.env.OF_SCRAPE_TTL_MINUTES, 10) : null;

// Database Configuration
const DATABASE_URL = process.env.DATABASE_URL;
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Allowed CDN hostnames for SSRF protection
const ALLOWED_CDN_HOSTS = ['.onlyfans.com', '.onlyfanscdn.com', '.ofsys.com', '.onlycdn.com'];

// Background loading state
let vaultLoadingStatus = {
  isLoading: false,
  progress: 0,
  total: 0,
  loaded: 0,
  errors: 0,
  startedAt: null,
  completedAt: null,
  accountId: null
};

// =========================
// Database Schema Setup
// =========================
async function initializeDatabase() {
  const client = await pool.connect();
  try {
    // Create tables if they don't exist
    await client.query(`
      CREATE TABLE IF NOT EXISTS media_cache (
        id SERIAL PRIMARY KEY,
        account_id VARCHAR(255) NOT NULL,
        media_id VARCHAR(255) NOT NULL,
        media_type VARCHAR(50) NOT NULL,
        cdn_url TEXT NOT NULL,
        scraped_url TEXT,
        preview_url TEXT,
        poster_url TEXT,
        expires_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        scrape_attempts INTEGER DEFAULT 0,
        last_error TEXT,
        metadata JSONB DEFAULT '{}',
        UNIQUE(account_id, media_id)
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS conversations (
        id SERIAL PRIMARY KEY,
        account_id VARCHAR(255) NOT NULL,
        chat_id VARCHAR(255) NOT NULL,
        user_id VARCHAR(255) NOT NULL,
        user_name VARCHAR(255),
        user_username VARCHAR(255),
        last_message_text TEXT,
        last_message_at TIMESTAMP,
        unread_count INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(account_id, chat_id)
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        account_id VARCHAR(255) NOT NULL,
        chat_id VARCHAR(255) NOT NULL,
        message_id VARCHAR(255),
        conversation_id INTEGER REFERENCES conversations(id),
        message_text TEXT,
        sent_by_account BOOLEAN DEFAULT false,
        has_media BOOLEAN DEFAULT false,
        media_count INTEGER DEFAULT 0,
        price DECIMAL(10,2) DEFAULT 0,
        is_tip BOOLEAN DEFAULT false,
        is_read BOOLEAN DEFAULT false,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        message_created_at TIMESTAMP,
        metadata JSONB DEFAULT '{}'
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS user_preferences (
        account_id VARCHAR(255) PRIMARY KEY,
        preferences JSONB DEFAULT '{}',
        ui_state JSONB DEFAULT '{}',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS account_stats (
        account_id VARCHAR(255) PRIMARY KEY,
        stats JSONB DEFAULT '{}',
        cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Create indexes for better performance
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_media_cache_account_id ON media_cache(account_id);
      CREATE INDEX IF NOT EXISTS idx_media_cache_expires_at ON media_cache(expires_at);
      CREATE INDEX IF NOT EXISTS idx_conversations_account_id ON conversations(account_id);
      CREATE INDEX IF NOT EXISTS idx_conversations_last_message_at ON conversations(last_message_at DESC);
      CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id);
      CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at DESC);
    `);

    console.log('[DB] Database initialized successfully');
  } catch (error) {
    console.error('[DB] Database initialization failed:', error);
    throw error;
  } finally {
    client.release();
  }
}

// =========================
// Middleware / Static
// =========================
app.use(express.json());
app.use(cors({
  origin: process.env.CORS_ORIGIN || '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  credentials: false
}));
app.use(express.static(path.join(__dirname, 'public')));

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
// Axios client
// =========================
const apiClient = axios.create({
  baseURL: OF_API_BASE,
  timeout: 120000,
  headers: {
    Authorization: `Bearer ${OF_API_KEY}`,
    'Content-Type': 'application/json',
  },
});

// =========================
// Helpers
// =========================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isAllowedCdnHost(hostname) {
  if (!hostname) return false;
  return ALLOWED_CDN_HOSTS.some(allowedHost => hostname.endsWith(allowedHost));
}

function isTransformedUrl(url) {
  if (!url) return true;
  const lowered = url.toLowerCase();
  
  const badMarkers = [
    '_preview', '_thumb', 'frame_0.jpg', '/thumbs/', '/previews/',
  ];
  if (badMarkers.some((m) => lowered.includes(m))) return true;

  const pathPart = lowered.split('?')[0];
  const fileName = pathPart.split('/').pop();
  
  if (/^\d{2,4}x\d{2,4}_/.test(fileName)) {
    const hasPreviewPath = lowered.includes('/thumbs/') || lowered.includes('/previews/');
    const hasPreviewSuffix = /_preview\.(jpg|jpeg|png|gif|webp)$/.test(lowered);
    
    if (hasPreviewPath || hasPreviewSuffix) {
      return true;
    }
  }

  return false;
}

function extractOriginalUrl(item) {
  const candidates = [
    item?.files?.full?.url,
    item?.files?.source?.url,
    item?.source?.source,
    typeof item?.source === 'string' ? item.source : null,
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (!isTransformedUrl(candidate)) {
      try {
        const parsedUrl = new URL(candidate);
        if (!isAllowedCdnHost(parsedUrl.hostname)) {
          continue;
        }
      } catch (e) {
        continue;
      }
      return candidate;
    }
  }
  return null;
}

function extractPosterCandidates(item) {
  const rawCandidates = [
    item?.files?.preview?.url,
    item?.files?.thumb?.url,
    item?.preview?.url,
    item?.thumb?.url,
    item?.preview,
    item?.thumb,
  ];

  const candidates = rawCandidates
    .map(candidate => {
      if (typeof candidate === 'string') return candidate;
      if (candidate && typeof candidate === 'object' && candidate.url) return String(candidate.url);
      return null;
    })
    .filter(Boolean);

  return candidates;
}

async function withRetries(fn, { attempts = 3, baseDelayMs = 500 } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const status = err.response?.status;
      const isTimeout = err.code === 'ECONNABORTED';
      const is408 = status === 408;
      const is5xx = status >= 500 && status < 600;

      if (!isTimeout && !is408 && !is5xx) break;

      const delay = baseDelayMs * Math.pow(2, i);
      const jitteredDelay = Math.round(delay * (0.8 + Math.random() * 0.4));
      
      console.warn(`[RETRY] Attempt ${i + 1} failed (${isTimeout ? 'timeout' : status}). Retrying in ${jitteredDelay}ms...`);
      await sleep(jitteredDelay);
    }
  }
  throw lastErr;
}

// =========================
// Database Operations
// =========================
async function getCachedMediaUrl(accountId, mediaId) {
  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT * FROM media_cache WHERE account_id = $1 AND media_id = $2 AND (expires_at IS NULL OR expires_at > NOW())',
      [accountId, mediaId]
    );
    return result.rows[0] || null;
  } finally {
    client.release();
  }
}

async function saveCachedMediaUrl(accountId, mediaId, data) {
  const client = await pool.connect();
  try {
    const expiresAt = new Date(Date.now() + OF_SCRAPE_EXPIRES_IN * 1000);
    
    await client.query(`
      INSERT INTO media_cache (account_id, media_id, media_type, cdn_url, scraped_url, preview_url, poster_url, expires_at, metadata, scrape_attempts)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT (account_id, media_id) 
      DO UPDATE SET 
        scraped_url = EXCLUDED.scraped_url,
        preview_url = EXCLUDED.preview_url,
        poster_url = EXCLUDED.poster_url,
        expires_at = EXCLUDED.expires_at,
        updated_at = CURRENT_TIMESTAMP,
        metadata = EXCLUDED.metadata,
        scrape_attempts = EXCLUDED.scrape_attempts,
        last_error = NULL
    `, [
      accountId,
      mediaId,
      data.type || 'unknown',
      data.cdnUrl || '',
      data.scrapedUrl || null,
      data.previewUrl || null,
      data.posterUrl || null,
      expiresAt,
      JSON.stringify(data.metadata || {}),
      data.scrapeAttempts || 0
    ]);
  } finally {
    client.release();
  }
}

async function recordScrapingError(accountId, mediaId, error, attempts) {
  const client = await pool.connect();
  try {
    await client.query(`
      UPDATE media_cache 
      SET last_error = $3, scrape_attempts = $4, updated_at = CURRENT_TIMESTAMP
      WHERE account_id = $1 AND media_id = $2
    `, [accountId, mediaId, error, attempts]);
  } finally {
    client.release();
  }
}

// =========================
// Media Scraping
// =========================
async function scrapeUrl(cdnUrl, accountId) {
  return scrapeLimiter.run(async () => {
    try {
      console.log(`[SCRAPE] -> ${cdnUrl.substring(0, 120)}...`);
      const body = {
        url: cdnUrl,
        cdn_url: cdnUrl,
      };

      if (OF_SCRAPE_TTL_MINUTES) {
        const expirationDate = new Date(Date.now() + OF_SCRAPE_TTL_MINUTES * 60 * 1000);
        body.expiration_date = expirationDate.toISOString();
      }

      const response = await withRetries(
        () => apiClient.post(`/api/${accountId}/media/scrape`, body),
        { attempts: 3, baseDelayMs: 700 }
      );

      const data = response.data || {};
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

      console.log(`[SCRAPE] OK -> ${scrapedUrl.substring(0, 60)}...`);
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

// =========================
// Background Vault Loading
// =========================
async function preloadVaultMedia(accountId) {
  if (vaultLoadingStatus.isLoading) {
    console.log(`[PRELOAD] Already loading vault for ${accountId}`);
    return;
  }

  console.log(`[PRELOAD] Starting vault media preload for ${accountId}`);
  vaultLoadingStatus = {
    isLoading: true,
    progress: 0,
    total: 0,
    loaded: 0,
    errors: 0,
    startedAt: new Date(),
    completedAt: null,
    accountId
  };

  // Emit loading start
  io.emit('vault-loading-start', vaultLoadingStatus);

  try {
    // Fetch all vault media
    console.log(`[PRELOAD] Fetching vault media list...`);
    const vaultResponse = await apiClient.get(`/api/${accountId}/media/vault`, {
      params: { limit: 1000, order: 'publish_date_desc' },
    });

    let items = [];
    if (vaultResponse.data?.data?.list) items = vaultResponse.data.data.list;
    else if (Array.isArray(vaultResponse.data?.data)) items = vaultResponse.data.data;
    else if (Array.isArray(vaultResponse.data)) items = vaultResponse.data;
    else if (vaultResponse.data?.list) items = vaultResponse.data.list;

    if (!Array.isArray(items)) {
      throw new Error('Invalid vault response structure');
    }

    vaultLoadingStatus.total = items.length;
    console.log(`[PRELOAD] Found ${items.length} media items to process`);

    // Process items in batches
    const batchSize = 10;
    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      
      await Promise.all(batch.map(async (item) => {
        try {
          await processMediaItem(item, accountId);
          vaultLoadingStatus.loaded++;
        } catch (error) {
          console.error(`[PRELOAD] Failed to process item ${item.id}:`, error.message);
          vaultLoadingStatus.errors++;
        }
        
        vaultLoadingStatus.progress = Math.round((vaultLoadingStatus.loaded + vaultLoadingStatus.errors) / vaultLoadingStatus.total * 100);
        
        // Emit progress update every 10 items
        if ((vaultLoadingStatus.loaded + vaultLoadingStatus.errors) % 10 === 0) {
          io.emit('vault-loading-progress', vaultLoadingStatus);
        }
      }));

      // Small delay between batches to avoid overwhelming the API
      await sleep(100);
    }

    vaultLoadingStatus.isLoading = false;
    vaultLoadingStatus.completedAt = new Date();
    
    console.log(`[PRELOAD] Completed! Loaded: ${vaultLoadingStatus.loaded}, Errors: ${vaultLoadingStatus.errors}`);
    io.emit('vault-loading-complete', vaultLoadingStatus);

  } catch (error) {
    console.error(`[PRELOAD] Failed:`, error);
    vaultLoadingStatus.isLoading = false;
    vaultLoadingStatus.completedAt = new Date();
    io.emit('vault-loading-error', { ...vaultLoadingStatus, error: error.message });
  }
}

async function processMediaItem(item, accountId) {
  const mediaId = item.id;
  
  // Check if already cached and valid
  const cached = await getCachedMediaUrl(accountId, mediaId);
  if (cached && cached.scraped_url) {
    return cached;
  }

  const mediaType = item.type || 'photo';
  const originalUrl = extractOriginalUrl(item);
  const posterCandidates = extractPosterCandidates(item);
  
  const cacheData = {
    type: mediaType,
    cdnUrl: originalUrl,
    previewUrl: posterCandidates[0] || null,
    posterUrl: posterCandidates[0] || null,
    metadata: {
      createdAt: item.createdAt || item.created_at,
      canView: item.canView !== false,
      locked: item.canView === false
    },
    scrapeAttempts: 0
  };

  if (!originalUrl) {
    console.log(`[PRELOAD] No original URL for item ${mediaId}`);
    cacheData.scrapedUrl = null;
    await saveCachedMediaUrl(accountId, mediaId, cacheData);
    return;
  }

  // Scrape the URL
  const scrapeResult = await scrapeUrl(originalUrl, accountId);
  
  if (scrapeResult.url) {
    cacheData.scrapedUrl = scrapeResult.url;
    await saveCachedMediaUrl(accountId, mediaId, cacheData);
  } else {
    cacheData.scrapeAttempts = 1;
    await saveCachedMediaUrl(accountId, mediaId, cacheData);
    await recordScrapingError(accountId, mediaId, scrapeResult.message, 1);
  }
}

// =========================
// Message Operations
// =========================
async function saveConversation(accountId, chatData) {
  const client = await pool.connect();
  try {
    const user = chatData.withUser || chatData.fan || {};
    await client.query(`
      INSERT INTO conversations (account_id, chat_id, user_id, user_name, user_username, is_active)
      VALUES ($1, $2, $3, $4, $5, true)
      ON CONFLICT (account_id, chat_id) 
      DO UPDATE SET 
        user_name = EXCLUDED.user_name,
        user_username = EXCLUDED.user_username,
        updated_at = CURRENT_TIMESTAMP
    `, [
      accountId,
      chatData.id || chatData.chatId,
      user.id,
      user.name,
      user.username
    ]);
  } finally {
    client.release();
  }
}

async function saveMessage(accountId, chatId, messageData) {
  const client = await pool.connect();
  try {
    // Get conversation ID
    const convResult = await client.query(
      'SELECT id FROM conversations WHERE account_id = $1 AND chat_id = $2',
      [accountId, chatId]
    );
    
    if (convResult.rows.length === 0) {
      console.warn(`[MESSAGES] Conversation not found: ${chatId}`);
      return;
    }
    
    const conversationId = convResult.rows[0].id;
    
    await client.query(`
      INSERT INTO messages (
        account_id, chat_id, message_id, conversation_id, message_text, 
        sent_by_account, has_media, media_count, price, is_tip, 
        message_created_at, metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      ON CONFLICT DO NOTHING
    `, [
      accountId,
      chatId,
      messageData.id,
      conversationId,
      messageData.text || messageData.message,
      messageData.fromUser?.id === accountId,
      messageData.media && messageData.media.length > 0,
      messageData.media ? messageData.media.length : 0,
      messageData.price || 0,
      messageData.isTip || false,
      messageData.createdAt || messageData.created_at,
      JSON.stringify(messageData)
    ]);

    // Update conversation last message
    await client.query(`
      UPDATE conversations 
      SET last_message_text = $3, last_message_at = $4, updated_at = CURRENT_TIMESTAMP
      WHERE account_id = $1 AND chat_id = $2
    `, [
      accountId,
      chatId,
      messageData.text || messageData.message || 'Media message',
      messageData.createdAt || messageData.created_at || new Date()
    ]);

  } finally {
    client.release();
  }
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
          id: OF_ACCOUNT_ID,
          numericId: accountData.id,
          username: String(accountData?.username || accountData?.name || OF_ACCOUNT_ID),
          name: String(accountData?.name || accountData?.username || OF_ACCOUNT_ID),
        });
        console.log(`[ACCOUNTS] Loaded account: ${OF_ACCOUNT_ID} (numeric: ${accountData.id})`);
      } catch (e) {
        console.error(`[ACCOUNTS] Failed to load details for ${OF_ACCOUNT_ID}: ${e.message}`);
        accounts.push({ id: OF_ACCOUNT_ID, username: OF_ACCOUNT_ID, name: OF_ACCOUNT_ID });
      }
    }

    res.json(accounts);
  } catch (error) {
    console.error('[ACCOUNTS] Error:', error.message);
    res.status(500).json({ error: 'Failed to fetch accounts' });
  }
});

// Vault media with cached data
app.get('/api/accounts/:accountId/media', async (req, res) => {
  try {
    const { accountId } = req.params;
    const { limit = 50, offset = 0 } = req.query;

    console.log(`[VAULT] Fetching cached media for ${accountId} | limit=${limit} offset=${offset}`);

    const client = await pool.connect();
    try {
      // Get cached media with pagination
      const result = await client.query(`
        SELECT 
          media_id as id,
          media_type as type,
          scraped_url,
          preview_url,
          poster_url,
          metadata,
          created_at,
          expires_at,
          last_error,
          scrape_attempts
        FROM media_cache 
        WHERE account_id = $1 
        ORDER BY created_at DESC 
        LIMIT $2 OFFSET $3
      `, [accountId, parseInt(limit), parseInt(offset)]);

      // Get total count
      const countResult = await client.query(
        'SELECT COUNT(*) FROM media_cache WHERE account_id = $1',
        [accountId]
      );

      const totalCount = parseInt(countResult.rows[0].count);
      const hasMore = (parseInt(offset) + parseInt(limit)) < totalCount;

      const media = result.rows.map(row => ({
        id: row.id,
        type: row.type,
        createdAt: row.metadata?.createdAt || row.created_at,
        canView: row.metadata?.canView !== false,
        locked: row.metadata?.locked || false,
        previewUrl: row.preview_url,
        posterUrl: row.poster_url,
        playbackUrl: row.scraped_url,
        downloadUrl: row.scraped_url,
        imageUrl: row.scraped_url,
        error: !row.scraped_url && row.last_error,
        errorCode: !row.scraped_url && row.last_error ? 'scrape_failed' : null,
        errorMessage: row.last_error,
        cached: true,
        expiresAt: row.expires_at,
        scrapeAttempts: row.scrape_attempts
      }));

      console.log(`[VAULT] Returned ${media.length} cached media items`);

      res.json({
        media,
        total: totalCount,
        hasMore,
        offset: parseInt(offset),
        limit: parseInt(limit),
        cached: true,
        loadingStatus: vaultLoadingStatus
      });
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('[VAULT] Error:', error.message);
    res.status(500).json({ error: 'Failed to fetch media', message: error.message });
  }
});

// Get vault loading status
app.get('/api/accounts/:accountId/vault-status', (req, res) => {
  res.json(vaultLoadingStatus);
});

// Trigger manual vault reload
app.post('/api/accounts/:accountId/reload-vault', async (req, res) => {
  const { accountId } = req.params;
  
  if (vaultLoadingStatus.isLoading) {
    return res.status(409).json({ error: 'Vault loading already in progress' });
  }

  // Clear existing cache for this account
  const client = await pool.connect();
  try {
    await client.query('DELETE FROM media_cache WHERE account_id = $1', [accountId]);
    console.log(`[VAULT] Cleared cache for ${accountId}`);
  } finally {
    client.release();
  }

  // Start preloading
  preloadVaultMedia(accountId).catch(console.error);
  
  res.json({ message: 'Vault reload started', status: vaultLoadingStatus });
});

// Get conversations
app.get('/api/accounts/:accountId/chats', async (req, res) => {
  try {
    const { accountId } = req.params;
    console.log('[CHATS] Fetching conversations');

    // Try to get fresh data from API
    try {
      const chatsResponse = await apiClient.get(`/api/${accountId}/chats`);
      const chats = Array.isArray(chatsResponse.data)
        ? chatsResponse.data
        : chatsResponse.data?.data || chatsResponse.data?.list || [];

      // Save conversations to database
      for (const chat of chats) {
        await saveConversation(accountId, chat);
      }
    } catch (apiError) {
      console.warn('[CHATS] API call failed, using cached data:', apiError.message);
    }

    // Return data from database
    const client = await pool.connect();
    try {
      const result = await client.query(`
        SELECT * FROM conversations 
        WHERE account_id = $1 AND is_active = true 
        ORDER BY last_message_at DESC NULLS LAST, updated_at DESC
        LIMIT 50
      `, [accountId]);

      const conversations = result.rows.map(row => ({
        id: row.chat_id,
        chatId: row.chat_id,
        userId: row.user_id,
        user: {
          id: row.user_id,
          name: row.user_name,
          username: row.user_username
        },
        lastMessage: {
          text: row.last_message_text,
          createdAt: row.last_message_at
        },
        unreadCount: row.unread_count,
        updatedAt: row.updated_at
      }));

      console.log(`[CHATS] Returning ${conversations.length} conversations`);
      res.json(conversations);
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('[CHATS] Error:', error.message);
    res.status(500).json({ error: 'Failed to fetch conversations' });
  }
});

// Get messages for a specific chat
app.get('/api/accounts/:accountId/chats/:chatId/messages', async (req, res) => {
  try {
    const { accountId, chatId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    console.log(`[MESSAGES] Fetching messages for chat ${chatId}`);

    // Try to get fresh data from API
    try {
      const messagesResponse = await apiClient.get(`/api/${accountId}/chats/${chatId}/messages?limit=100`);
      const messages = Array.isArray(messagesResponse.data)
        ? messagesResponse.data
        : messagesResponse.data?.data || messagesResponse.data?.list || [];

      // Save messages to database
      for (const message of messages) {
        await saveMessage(accountId, chatId, message);
      }
    } catch (apiError) {
      console.warn('[MESSAGES] API call failed, using cached data:', apiError.message);
    }

    // Return data from database
    const client = await pool.connect();
    try {
      const result = await client.query(`
        SELECT m.*, c.user_name, c.user_username 
        FROM messages m
        LEFT JOIN conversations c ON m.conversation_id = c.id
        WHERE m.account_id = $1 AND m.chat_id = $2 
        ORDER BY m.message_created_at DESC 
        LIMIT $3 OFFSET $4
      `, [accountId, chatId, parseInt(limit), parseInt(offset)]);

      const messages = result.rows.map(row => ({
        id: row.message_id,
        text: row.message_text,
        createdAt: row.message_created_at,
        sentByAccount: row.sent_by_account,
        hasMedia: row.has_media,
        mediaCount: row.media_count,
        price: parseFloat(row.price || 0),
        isTip: row.is_tip,
        isRead: row.is_read,
        fromUser: {
          id: row.sent_by_account ? accountId : row.user_id,
          name: row.sent_by_account ? 'You' : row.user_name,
          username: row.sent_by_account ? null : row.user_username
        }
      }));

      console.log(`[MESSAGES] Returning ${messages.length} messages for chat ${chatId}`);
      res.json(messages);
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('[MESSAGES] Error:', error.message);
    res.status(500).json({ error: 'Failed to fetch messages' });
  }
});

// Send a message
app.post('/api/accounts/:accountId/chats/:chatId/messages', async (req, res) => {
  try {
    const { accountId, chatId } = req.params;
    const { text, price } = req.body;

    if (!text) {
      return res.status(400).json({ error: 'Message text is required' });
    }

    console.log(`[MESSAGES] Sending message to chat ${chatId}`);

    const messageData = {
      text: text.trim(),
    };

    if (price && price > 0) {
      messageData.price = parseFloat(price);
    }

    // Send via API
    const response = await apiClient.post(`/api/${accountId}/chats/${chatId}/messages`, messageData);
    const sentMessage = response.data;

    // Save to database
    const messageToSave = {
      id: sentMessage.id || `sent_${Date.now()}`,
      text: text,
      createdAt: new Date().toISOString(),
      fromUser: { id: accountId },
      price: price || 0
    };
    
    await saveMessage(accountId, chatId, messageToSave);

    // Emit real-time update
    io.emit('new-message', {
      accountId,
      chatId,
      message: messageToSave
    });

    console.log(`[MESSAGES] Message sent successfully`);
    res.json({ 
      success: true, 
      message: messageToSave,
      apiResponse: sentMessage
    });

  } catch (error) {
    console.error('[MESSAGES] Send error:', error.message);
    const status = error.response?.status || 500;
    const message = error.response?.data?.message || error.message;
    res.status(status).json({ error: 'Failed to send message', message });
  }
});

// Legacy messages endpoint for backward compatibility
app.get('/api/accounts/:accountId/messages', async (req, res) => {
  try {
    const { accountId } = req.params;
    console.log('[MESSAGES] Fetching recent messages (legacy endpoint)');

    const client = await pool.connect();
    try {
      const result = await client.query(`
        SELECT m.*, c.user_name, c.user_username, c.chat_id
        FROM messages m
        LEFT JOIN conversations c ON m.conversation_id = c.id
        WHERE m.account_id = $1 
        ORDER BY m.message_created_at DESC 
        LIMIT 50
      `, [accountId]);

      const messages = result.rows.map(row => ({
        id: row.message_id,
        text: row.message_text,
        createdAt: row.message_created_at,
        fromUser: {
          id: row.sent_by_account ? accountId : row.user_id,
          name: row.sent_by_account ? 'You' : row.user_name,
        },
        chatId: row.chat_id
      }));

      res.json(messages);
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('[MESSAGES] Error:', error.message);
    res.json([]);
  }
});

// Activity (enhanced)
app.get('/api/accounts/:accountId/activity', async (req, res) => {
  console.log('[ACTIVITY] Fetching recent activity');
  try {
    const { accountId } = req.params;
    const activities = [];

    // Get activities from various sources
    try {
      const subsResponse = await apiClient.get(`/api/${accountId}/subscribers/active?limit=10`);
      const subs = Array.isArray(subsResponse.data)
        ? subsResponse.data
        : subsResponse.data?.data || subsResponse.data?.list || [];

      subs.forEach((sub) => {
        activities.push({
          type: 'subscription',
          title: 'New Subscription',
          description: `New subscriber: ${String(sub?.name || sub?.username || 'Anonymous')}`,
          amount: sub?.currentSubscribePrice || sub?.subscribePrice || 0,
          timestamp: sub?.subscribedOnData?.subscribedAt || sub?.subscribedAt || new Date().toISOString(),
          userId: sub?.id,
          userName: sub?.name || sub?.username
        });
      });
    } catch (e) {
      console.error('[ACTIVITY] Subscribers error:', e.message);
    }

    // Get recent messages as activity
    try {
      const client = await pool.connect();
      try {
        const messageResult = await client.query(`
          SELECT m.*, c.user_name 
          FROM messages m
          LEFT JOIN conversations c ON m.conversation_id = c.id
          WHERE m.account_id = $1 AND m.sent_by_account = false
          ORDER BY m.message_created_at DESC 
          LIMIT 10
        `, [accountId]);

        messageResult.rows.forEach(msg => {
          activities.push({
            type: 'message',
            title: 'New Message',
            description: `Message from ${msg.user_name || 'Unknown'}`,
            timestamp: msg.message_created_at,
            userId: msg.user_id,
            userName: msg.user_name
          });
        });
      } finally {
        client.release();
      }
    } catch (e) {
      console.error('[ACTIVITY] Messages activity error:', e.message);
    }

    activities.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    console.log(`[ACTIVITY] Returning ${activities.length} activities`);
    res.json(activities.slice(0, 20));
  } catch (error) {
    console.error('[ACTIVITY] Error:', error.message);
    res.json([]);
  }
});

// Stats with caching
app.get('/api/accounts/:accountId/stats', async (req, res) => {
  console.log('[STATS] Fetching account statistics');
  try {
    const { accountId } = req.params;
    
    // Try to get fresh data
    let stats = {
      subscribers: 0,
      revenueToday: '0.00',
      postsCount: 0,
      photosCount: 0,
      videosCount: 0,
      totalMedia: 0,
      unreadMessages: 0,
      vaultLoadingStatus: vaultLoadingStatus
    };

    try {
      const accountResponse = await apiClient.get(`/api/${accountId}/me`);
      const accountData = accountResponse.data?.data || accountResponse.data || {};
      stats.subscribers = accountData?.subscribersCount || accountData?.counters?.subscribersCount || 0;
      stats.postsCount = accountData?.postsCount || accountData?.counters?.postsCount || 0;
      stats.photosCount = accountData?.photosCount || accountData?.counters?.photosCount || 0;
      stats.videosCount = accountData?.videosCount || accountData?.counters?.videosCount || 0;
    } catch (e) {
      console.error('[STATS] Account data error:', e.message);
    }

    // Get media count from cache
    const client = await pool.connect();
    try {
      const mediaCountResult = await client.query(
        'SELECT COUNT(*) FROM media_cache WHERE account_id = $1',
        [accountId]
      );
      stats.totalMedia = parseInt(mediaCountResult.rows[0].count);

      const unreadResult = await client.query(
        'SELECT SUM(unread_count) FROM conversations WHERE account_id = $1',
        [accountId]
      );
      stats.unreadMessages = parseInt(unreadResult.rows[0].sum || 0);

      // Cache stats
      await client.query(`
        INSERT INTO account_stats (account_id, stats) 
        VALUES ($1, $2) 
        ON CONFLICT (account_id) 
        DO UPDATE SET stats = EXCLUDED.stats, cached_at = CURRENT_TIMESTAMP
      `, [accountId, JSON.stringify(stats)]);

    } finally {
      client.release();
    }

    res.json(stats);
  } catch (error) {
    console.error('[STATS] Error:', error.message);
    
    // Try to return cached stats
    try {
      const client = await pool.connect();
      try {
        const result = await client.query(
          'SELECT stats FROM account_stats WHERE account_id = $1',
          [req.params.accountId]
        );
        if (result.rows.length > 0) {
          return res.json({
            ...result.rows[0].stats,
            cached: true,
            vaultLoadingStatus: vaultLoadingStatus
          });
        }
      } finally {
        client.release();
      }
    } catch (cacheError) {
      console.error('[STATS] Cache error:', cacheError.message);
    }

    res.json({
      subscribers: 0,
      revenueToday: '0.00',
      postsCount: 0,
      photosCount: 0,
      videosCount: 0,
      totalMedia: 0,
      unreadMessages: 0,
      vaultLoadingStatus: vaultLoadingStatus,
      error: true
    });
  }
});

// Single scrape endpoint
app.post('/api/accounts/:accountId/media/scrape-single', async (req, res) => {
  try {
    const { accountId } = req.params;
    const { cdn_url } = req.body;
    if (!cdn_url) return res.status(400).json({ error: 'cdn_url required' });

    try {
      const parsedUrl = new URL(cdn_url);
      if (!isAllowedCdnHost(parsedUrl.hostname)) {
        return res.status(422).json({ 
          error: 'invalid_host', 
          message: 'Only OnlyFans CDN URLs are allowed.' 
        });
      }
    } catch (e) {
      return res.status(422).json({ 
        error: 'invalid_url', 
        message: 'Invalid URL format.' 
      });
    }

    if (isTransformedUrl(cdn_url)) {
      return res.status(422).json({ 
        error: 'not_original_url', 
        message: 'Provide an original CDN URL, not a preview/thumb.' 
      });
    }

    const result = await scrapeUrl(cdn_url, accountId);
    if (result.errorCode) return res.status(502).json(result);
    res.json({ url: result.url });
  } catch (error) {
    console.error('[SCRAPE-SINGLE] Error:', error.message);
    res.status(500).json({ error: 'Failed to scrape URL', message: error.message });
  }
});

// Get scraped URL for specific media
app.get('/api/accounts/:accountId/media/scraped-url', async (req, res) => {
  try {
    const { accountId } = req.params;
    const { id } = req.query;
    if (!id) return res.status(400).json({ error: 'Media ID required' });

    console.log(`[SCRAPED-URL] Item ${id}`);

    const cached = await getCachedMediaUrl(accountId, id);
    if (cached && cached.scraped_url) {
      return res.json({
        url: cached.scraped_url,
        type: cached.media_type,
        cached: true,
        expiresAt: cached.expires_at
      });
    }

    res.status(404).json({ error: 'Media not found or not scraped yet' });
  } catch (error) {
    console.error('[SCRAPED-URL] Error:', error.message);
    res.status(500).json({ error: 'Failed to get scraped URL' });
  }
});

// Health endpoint
app.get('/api/health', async (req, res) => {
  let dbStatus = 'unknown';
  let cacheCount = 0;
  
  try {
    const client = await pool.connect();
    try {
      await client.query('SELECT 1');
      dbStatus = 'connected';
      
      const result = await client.query('SELECT COUNT(*) FROM media_cache');
      cacheCount = parseInt(result.rows[0].count);
    } finally {
      client.release();
    }
  } catch (e) {
    dbStatus = 'error';
  }

  res.json({
    status: 'ok',
    database: dbStatus,
    config: {
      apiBase: OF_API_BASE,
      accountId: OF_ACCOUNT_ID,
      apiKeySet: !!OF_API_KEY,
      scrapeOnList: OF_SCRAPE_ON_LIST,
      scrapeExpiresIn: OF_SCRAPE_EXPIRES_IN,
      scrapeConcurrency: OF_SCRAPE_CONCURRENCY,
      scrapeTtlMinutes: OF_SCRAPE_TTL_MINUTES,
      cacheSize: cacheCount,
      timeoutMs: apiClient.defaults.timeout,
    },
    vaultLoadingStatus: vaultLoadingStatus,
    uptime: process.uptime()
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
    
    // Send current vault loading status
    socket.emit('vault-loading-status', vaultLoadingStatus);
  });

  socket.on('disconnect', () => {
    console.log('[SOCKET] Client disconnected');
  });
});

// =========================
// Startup
// =========================
async function startServer() {
  try {
    // Initialize database
    await initializeDatabase();
    
    // Start server
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, async () => {
      console.log(`[SERVER] Started on port ${PORT}`);
      console.log(`[CONFIG] API Base: ${OF_API_BASE}`);
      console.log(`[CONFIG] Account ID: ${OF_ACCOUNT_ID}`);
      console.log(`[CONFIG] API Key: ${OF_API_KEY ? 'Set' : 'Not set'}`);
      console.log(`[CONFIG] Scrape expires in: ${OF_SCRAPE_EXPIRES_IN}s (${Math.round(OF_SCRAPE_EXPIRES_IN / 31536000)} years)`);
      
      // Start background vault preload if account is configured
      if (OF_ACCOUNT_ID) {
        console.log(`[STARTUP] Starting background vault preload for ${OF_ACCOUNT_ID}`);
        setTimeout(() => {
          preloadVaultMedia(OF_ACCOUNT_ID).catch(console.error);
        }, 5000); // 5 second delay to let server fully start
      }
    });
    
  } catch (error) {
    console.error('[STARTUP] Failed to start server:', error);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('[SHUTDOWN] Received SIGINT, shutting down gracefully');
  try {
    await pool.end();
    console.log('[SHUTDOWN] Database pool closed');
    process.exit(0);
  } catch (error) {
    console.error('[SHUTDOWN] Error during shutdown:', error);
    process.exit(1);
  }
});

process.on('SIGTERM', async () => {
  console.log('[SHUTDOWN] Received SIGTERM, shutting down gracefully');
  try {
    await pool.end();
    console.log('[SHUTDOWN] Database pool closed');
    process.exit(0);
  } catch (error) {
    console.error('[SHUTDOWN] Error during shutdown:', error);
    process.exit(1);
  }
});

startServer();
