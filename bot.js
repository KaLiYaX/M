// Load environment variables
require('dotenv').config();

const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const FormData = require('form-data');
const schedule = require('node-schedule');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'kalindu_gaweshana';

// Multi-page support - Format: PAGE_ID1,PAGE_ID2,PAGE_ID3 or single PAGE_ID
const PAGE_IDS = process.env.PAGE_ID ? process.env.PAGE_ID.split(',').map(id => id.trim()) : [];
const PAGE_NAMES = process.env.PAGE_NAMES ? process.env.PAGE_NAMES.split(',').map(name => name.trim()) : [];

// Data file paths
const DATA_DIR = path.join(__dirname, 'data');
const HISTORY_FILE = path.join(DATA_DIR, 'processed_videos.json');
const ANALYTICS_FILE = path.join(DATA_DIR, 'analytics.json');

if (!TELEGRAM_TOKEN || !PAGE_ACCESS_TOKEN || PAGE_IDS.length === 0) {
  console.error('❌ Missing required environment variables!');
  console.log('Required: TELEGRAM_TOKEN, PAGE_ACCESS_TOKEN, PAGE_ID');
  console.log('Format for multiple pages: PAGE_ID=123,456,789');
  console.log('Optional: PAGE_NAMES=Page1,Page2,Page3');
  process.exit(1);
}

// Create default page names if not provided
while (PAGE_NAMES.length < PAGE_IDS.length) {
  PAGE_NAMES.push(`Page ${PAGE_NAMES.length + 1}`);
}

const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });
const YOUTUBE_API_BASE = 'https://youtube-apis.vercel.app/api/ytmp4';
const YOUTUBE_SEARCH_API = 'https://youtube-apis.vercel.app/api/search';

let ADMIN_ID = null;
const videoQueue = [];
let processedVideos = new Set();
let analytics = {
  totalVideos: 0,
  successfulPosts: 0,
  failedPosts: 0,
  totalSize: 0,
  shortsCount: 0,
  duplicatesSkipped: 0,
  searchesPerformed: 0,
  startTime: Date.now(),
  lastSaved: null,
  pageStats: {} // Track stats per page
};
const scheduledPosts = [];
const userSessions = new Map();
const activeDownloads = new Map();
const searchCache = new Map();

// ============================================
// FILE MANAGEMENT
// ============================================

async function ensureDataDirectory() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    console.log('📁 Data directory ready');
  } catch (error) {
    console.error('❌ Failed to create data directory:', error.message);
  }
}

async function loadProcessedVideos() {
  try {
    const data = await fs.readFile(HISTORY_FILE, 'utf8');
    const parsed = JSON.parse(data);
    processedVideos = new Set(parsed.videos || []);
    console.log(`📂 Loaded ${processedVideos.size} processed videos from history`);
  } catch (error) {
    if (error.code === 'ENOENT') {
      console.log('📂 No history file found, starting fresh');
      processedVideos = new Set();
    } else {
      console.error('❌ Error loading history:', error.message);
      processedVideos = new Set();
    }
  }
}

async function saveProcessedVideos() {
  try {
    const data = {
      videos: Array.from(processedVideos),
      lastUpdated: new Date().toISOString(),
      count: processedVideos.size
    };
    await fs.writeFile(HISTORY_FILE, JSON.stringify(data, null, 2));
    console.log(`💾 Saved ${processedVideos.size} videos to history`);
  } catch (error) {
    console.error('❌ Error saving history:', error.message);
  }
}

async function loadAnalytics() {
  try {
    const data = await fs.readFile(ANALYTICS_FILE, 'utf8');
    const parsed = JSON.parse(data);
    analytics = {
      ...analytics,
      ...parsed,
      startTime: parsed.startTime || Date.now()
    };
    console.log('📊 Loaded analytics from file');
  } catch (error) {
    if (error.code === 'ENOENT') {
      console.log('📊 No analytics file found, starting fresh');
    } else {
      console.error('❌ Error loading analytics:', error.message);
    }
  }
}

async function saveAnalytics() {
  try {
    analytics.lastSaved = new Date().toISOString();
    await fs.writeFile(ANALYTICS_FILE, JSON.stringify(analytics, null, 2));
    console.log('💾 Analytics saved');
  } catch (error) {
    console.error('❌ Error saving analytics:', error.message);
  }
}

// Auto-save every 5 minutes
setInterval(async () => {
  await saveProcessedVideos();
  await saveAnalytics();
}, 5 * 60 * 1000);

// ============================================
// YOUTUBE URL DETECTION
// ============================================

function detectYouTubeUrl(text) {
  const patterns = [
    { regex: /(https?:\/\/)?(www\.)?(youtube\.com\/watch\?v=)([a-zA-Z0-9_-]{11})/g, type: 'regular' },
    { regex: /(https?:\/\/)?(youtu\.be\/)([a-zA-Z0-9_-]{11})/g, type: 'regular' },
    { regex: /(https?:\/\/)?(www\.)?(youtube\.com\/shorts\/)([a-zA-Z0-9_-]{11})/g, type: 'shorts' },
    { regex: /(https?:\/\/)?(m\.youtube\.com\/watch\?v=)([a-zA-Z0-9_-]{11})/g, type: 'regular' }
  ];

  const matches = [];
  patterns.forEach(({ regex, type }) => {
    const found = [...text.matchAll(regex)];
    found.forEach(match => {
      const videoId = match[match.length - 1];
      matches.push({ 
        url: match[0], 
        videoId: videoId,
        type: type
      });
    });
  });

  return matches;
}

// ============================================
// YOUTUBE SEARCH
// ============================================

async function searchYouTube(query) {
  try {
    console.log('🔍 Searching YouTube for:', query);
    analytics.searchesPerformed++;
    
    const response = await axios.get(YOUTUBE_SEARCH_API, {
      params: { q: query },
      timeout: 30000
    });

    if (!response.data.status || !response.data.data.results) {
      throw new Error('No results found');
    }

    // Filter only video results (exclude channels)
    const videos = response.data.data.results.filter(r => r.type === 'video');
    
    console.log(`✅ Found ${videos.length} videos`);
    return videos;
  } catch (error) {
    console.error('❌ Search error:', error.message);
    throw new Error('Search failed: ' + error.message);
  }
}

// ============================================
// HELPER FUNCTIONS
// ============================================

function isAdmin(msg) {
  if (ADMIN_ID && msg.from.id === ADMIN_ID) return true;
  if (msg.from.username === ADMIN_USERNAME) {
    ADMIN_ID = msg.from.id;
    return true;
  }
  return false;
}

function getUserSession(userId) {
  if (!userSessions.has(userId)) {
    userSessions.set(userId, {
      customCaption: null,
      selectedQuality: '360',
      lastVideoData: null,
      pendingDuplicates: [],
      searchResults: [],
      lastSearchQuery: null,
      selectedPages: PAGE_IDS.length === 1 ? [0] : [] // Auto-select if only one page
    });
  }
  return userSessions.get(userId);
}

function isAlreadyProcessed(videoId) {
  return processedVideos.has(videoId);
}

function isInQueue(videoId) {
  return videoQueue.some(item => item.videoId === videoId);
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

function formatSpeed(bytesPerSecond) {
  return formatBytes(bytesPerSecond) + '/s';
}

function getProgressBar(percent) {
  const filled = Math.round(percent / 5);
  const empty = 20 - filled;
  return '█'.repeat(filled) + '░'.repeat(empty);
}

function formatViews(views) {
  if (views >= 1000000) return (views / 1000000).toFixed(1) + 'M';
  if (views >= 1000) return (views / 1000).toFixed(1) + 'K';
  return views.toString();
}

function formatDuration(seconds) {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}:${secs.toString().padStart(2, '0')}`;
}

// ============================================
// DOWNLOAD THUMBNAIL
// ============================================

async function downloadThumbnail(thumbnailUrl) {
  try {
    console.log('🖼️ Downloading thumbnail from:', thumbnailUrl);
    
    const response = await axios.get(thumbnailUrl, {
      responseType: 'arraybuffer',
      timeout: 30000
    });
    
    const buffer = Buffer.from(response.data);
    console.log('✅ Thumbnail downloaded:', formatBytes(buffer.length));
    
    return buffer;
  } catch (error) {
    console.error('❌ Thumbnail download error:', error.message);
    return null;
  }
}

// ============================================
// KEYBOARDS
// ============================================

const keyboards = {
  main: () => ({
    inline_keyboard: [
      [{ text: '🔍 Search Videos', callback_data: 'search_videos' }, { text: '📹 Add by URL', callback_data: 'add_video' }],
      [{ text: '📋 View Queue', callback_data: 'view_queue' }, { text: '📊 Analytics', callback_data: 'analytics' }],
      [{ text: '📄 Select Pages', callback_data: 'select_pages' }, { text: '⚙️ Settings', callback_data: 'settings' }],
      [{ text: '❓ Help', callback_data: 'help' }]
    ]
  }),
  
  pageSelection: (selectedPages) => {
    const buttons = [];
    
    PAGE_IDS.forEach((pageId, index) => {
      const isSelected = selectedPages.includes(index);
      const pageName = PAGE_NAMES[index] || `Page ${index + 1}`;
      buttons.push([{
        text: `${isSelected ? '✅' : '⬜'} ${pageName}`,
        callback_data: `toggle_page_${index}`
      }]);
    });
    
    buttons.push([
      { text: '✅ Select All', callback_data: 'select_all_pages' },
      { text: '❌ Deselect All', callback_data: 'deselect_all_pages' }
    ]);
    buttons.push([{ text: '🔙 Main Menu', callback_data: 'main_menu' }]);
    
    return { inline_keyboard: buttons };
  },
  
  searchResults: (results, page = 0, totalPages = 1) => {
    const buttons = [];
    const start = page * 5;
    const end = Math.min(start + 5, results.length);
    
    for (let i = start; i < end; i++) {
      const video = results[i];
      const icon = video.type === 'shorts' ? '📱' : '🎬';
      const title = video.title.substring(0, 35);
      buttons.push([{ 
        text: `${icon} ${title}...`, 
        callback_data: `select_video_${i}` 
      }]);
    }
    
    // Navigation
    const nav = [];
    if (page > 0) nav.push({ text: '⬅️ Previous', callback_data: `search_page_${page - 1}` });
    if (page < totalPages - 1) nav.push({ text: 'Next ➡️', callback_data: `search_page_${page + 1}` });
    if (nav.length > 0) buttons.push(nav);
    
    buttons.push([{ text: '🔍 New Search', callback_data: 'search_videos' }, { text: '🔙 Menu', callback_data: 'main_menu' }]);
    
    return { inline_keyboard: buttons };
  },
  
  videoConfirm: (index, isDuplicate) => ({
    inline_keyboard: [
      [{ text: '✅ Add to Queue', callback_data: `confirm_video_${index}` }],
      isDuplicate ? [{ text: '⚠️ Duplicate - Add Anyway?', callback_data: `confirm_duplicate_${index}` }] : [],
      [{ text: '🔙 Back to Results', callback_data: 'back_to_search' }]
    ].filter(row => row.length > 0)
  }),
  
  quality: (current) => {
    const qualities = ['144', '240', '360', '480', '720', '1080'];
    const buttons = [];
    for (let i = 0; i < qualities.length; i += 3) {
      const row = qualities.slice(i, i + 3).map(q => ({
        text: `${q}p${q === current ? ' ✓' : ''}`,
        callback_data: `quality_${q}`
      }));
      buttons.push(row);
    }
    buttons.push([{ text: '🔙 Back', callback_data: 'settings' }]);
    return { inline_keyboard: buttons };
  },
  
  settings: () => ({
    inline_keyboard: [
      [{ text: '🎬 Quality', callback_data: 'quality_settings' }, { text: '✍️ Caption', callback_data: 'set_caption' }],
      [{ text: '🗑️ Clear Queue', callback_data: 'clear_queue' }, { text: '🧹 Clear History', callback_data: 'clear_history' }],
      [{ text: '💾 Save Data', callback_data: 'save_data' }, { text: '📊 Data Info', callback_data: 'data_info' }],
      [{ text: '🔙 Main Menu', callback_data: 'main_menu' }]
    ]
  }),
  
  duplicateConfirm: (videoId) => ({
    inline_keyboard: [
      [{ text: '✅ Yes, Add Again', callback_data: `duplicate_confirm_${videoId}` }],
      [{ text: '❌ No, Skip', callback_data: 'duplicate_skip' }]
    ]
  }),
  
  downloadControl: (videoId, isPaused) => ({
    inline_keyboard: [
      [{ text: isPaused ? '▶️ Resume' : '⏸️ Pause', callback_data: `download_${isPaused ? 'resume' : 'pause'}_${videoId}` }],
      [{ text: '❌ Cancel', callback_data: `download_cancel_${videoId}` }]
    ]
  })
};

// ============================================
// COMMANDS
// ============================================

bot.onText(/\/start/, (msg) => {
  if (!isAdmin(msg)) {
    return bot.sendMessage(msg.chat.id, '❌ Admin Only Bot\n🔐 Contact: @' + ADMIN_USERNAME);
  }

  bot.sendMessage(msg.chat.id, `
👋 *Welcome ${msg.from.first_name}!*

🤖 *YouTube to Facebook Bot - ULTIMATE*

📄 *Multi-Page Support*
✅ ${PAGE_IDS.length} page(s) configured
✅ Post to multiple pages at once
✅ Select pages per video

✨ *NEW: Search Feature!*
🔍 Search videos directly in bot
🖼️ Preview with thumbnails
📊 See views, duration, channel

✅ Regular YouTube videos
✅ YouTube Shorts 📱
✅ No size limits 🚀
✅ Pause/Resume downloads ⏯️
✅ Smart progress (3-10s) 📊
✅ Auto posting to Facebook
✅ Duplicate detection 🔍
✅ Persistent history 💾
✅ YouTube thumbnails 🖼️

*Configured Pages:*
${PAGE_IDS.map((id, i) => `${i + 1}. ${PAGE_NAMES[i]}`).join('\n')}

*Features:*
🔍 Search & preview before adding
📄 Post to multiple pages
💾 Data saved automatically
🔄 Survives bot restarts
🔍 Smart duplicate detection
⏯️ Pause/Resume downloads
📊 Progress updates every 3-10s
🚀 Unlimited file sizes
🖼️ Auto thumbnail from YouTube

*Two Ways to Add Videos:*
1️⃣ 🔍 Search videos in bot
2️⃣ 📹 Paste YouTube URL directly
  `, { parse_mode: 'Markdown', reply_markup: keyboards.main() });
});

// ============================================
// CALLBACK HANDLER
// ============================================

bot.on('callback_query', async (query) => {
  const { message: msg, data, from } = query;
  if (!isAdmin(query)) return bot.answerCallbackQuery(query.id, { text: '❌ Admin only!' });
  
  bot.answerCallbackQuery(query.id);
  const session = getUserSession(from.id);

  try {
    if (data === 'main_menu') {
      await bot.editMessageText('*🏠 Main Menu*\n\nSelect an option:', {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: keyboards.main()
      });
    }
    
    else if (data === 'select_pages') {
      const selectedCount = session.selectedPages.length;
      await bot.editMessageText(
        `📄 *Select Pages*\n\n` +
        `Selected: ${selectedCount}/${PAGE_IDS.length}\n\n` +
        `Videos will be posted to all selected pages.\n\n` +
        `Click to toggle:`,
        {
          chat_id: msg.chat.id,
          message_id: msg.message_id,
          parse_mode: 'Markdown',
          reply_markup: keyboards.pageSelection(session.selectedPages)
        }
      );
    }
    
    else if (data.startsWith('toggle_page_')) {
      const pageIndex = parseInt(data.replace('toggle_page_', ''));
      const idx = session.selectedPages.indexOf(pageIndex);
      
      if (idx > -1) {
        session.selectedPages.splice(idx, 1);
      } else {
        session.selectedPages.push(pageIndex);
      }
      
      const selectedCount = session.selectedPages.length;
      await bot.editMessageText(
        `📄 *Select Pages*\n\n` +
        `Selected: ${selectedCount}/${PAGE_IDS.length}\n\n` +
        `Videos will be posted to all selected pages.\n\n` +
        `Click to toggle:`,
        {
          chat_id: msg.chat.id,
          message_id: msg.message_id,
          parse_mode: 'Markdown',
          reply_markup: keyboards.pageSelection(session.selectedPages)
        }
      );
    }
    
    else if (data === 'select_all_pages') {
      session.selectedPages = PAGE_IDS.map((_, i) => i);
      await bot.editMessageText(
        `📄 *Select Pages*\n\n` +
        `Selected: ${session.selectedPages.length}/${PAGE_IDS.length}\n\n` +
        `Videos will be posted to all selected pages.\n\n` +
        `Click to toggle:`,
        {
          chat_id: msg.chat.id,
          message_id: msg.message_id,
          parse_mode: 'Markdown',
          reply_markup: keyboards.pageSelection(session.selectedPages)
        }
      );
    }
    
    else if (data === 'deselect_all_pages') {
      session.selectedPages = [];
      await bot.editMessageText(
        `📄 *Select Pages*\n\n` +
        `Selected: 0/${PAGE_IDS.length}\n\n` +
        `Videos will be posted to all selected pages.\n\n` +
        `Click to toggle:`,
        {
          chat_id: msg.chat.id,
          message_id: msg.message_id,
          parse_mode: 'Markdown',
          reply_markup: keyboards.pageSelection(session.selectedPages)
        }
      );
    }
    
    else if (data === 'search_videos') {
      await bot.editMessageText(
        '🔍 *Search YouTube Videos*\n\n' +
        '✨ Search by keywords\n' +
        '📊 See views, duration & channel\n' +
        '🖼️ Preview thumbnails\n' +
        '✅ Select & add to queue\n\n' +
        '*Send your search query:*\n' +
        'Example: "lelena", "sinhala songs", etc.',
        {
          chat_id: msg.chat.id, 
          message_id: msg.message_id, 
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '🔙 Back', callback_data: 'main_menu' }]] }
        }
      );
      session.waitingForSearch = true;
    }
    
    else if (data === 'add_video') {
      session.waitingForSearch = false;
      await bot.editMessageText(
        '📹 *Add Video by URL - UNLIMITED*\n\n✅ No size limits\n✅ Shorts & Regular videos 📱\n✅ Pause/Resume support ⏯️\n✅ Smart progress (3-10s)\n🔍 Duplicate detection enabled\n🖼️ Auto thumbnail support\n\nSend YouTube links:',
        { chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '🔙 Back', callback_data: 'main_menu' }]] }
        }
      );
    }
    
    else if (data.startsWith('search_page_')) {
      const page = parseInt(data.replace('search_page_', ''));
      const totalPages = Math.ceil(session.searchResults.length / 5);
      
      await bot.editMessageText(
        `🔍 *Search Results* (Page ${page + 1}/${totalPages})\n\n` +
        `Query: "${session.lastSearchQuery}"\n` +
        `Found: ${session.searchResults.length} videos\n\n` +
        `Select a video:`,
        {
          chat_id: msg.chat.id,
          message_id: msg.message_id,
          parse_mode: 'Markdown',
          reply_markup: keyboards.searchResults(session.searchResults, page, totalPages)
        }
      );
    }
    
    else if (data.startsWith('select_video_')) {
      const index = parseInt(data.replace('select_video_', ''));
      const video = session.searchResults[index];
      
      if (!video) {
        return bot.answerCallbackQuery(query.id, { text: '❌ Video not found' });
      }
      
      const isDuplicate = isAlreadyProcessed(video.videoId);
      const inQueue = isInQueue(video.videoId);
      const icon = video.type === 'shorts' ? '📱' : '🎬';
      
      let statusText = '';
      if (isDuplicate) statusText = '\n\n⚠️ *DUPLICATE* - Already posted';
      else if (inQueue) statusText = '\n\n⏳ *IN QUEUE* - Already added';
      
      // Send video thumbnail
      try {
        await bot.sendPhoto(msg.chat.id, video.thumbnail || video.image, {
          caption: 
            `${icon} *${video.title}*\n\n` +
            `👤 ${video.author.name}\n` +
            `👁️ ${formatViews(video.views)} views\n` +
            `⏱️ ${video.duration.timestamp}\n` +
            `📅 ${video.ago}` +
            statusText,
          parse_mode: 'Markdown',
          reply_markup: inQueue ? 
            { inline_keyboard: [[{ text: '🔙 Back to Results', callback_data: 'back_to_search' }]] } :
            keyboards.videoConfirm(index, isDuplicate)
        });
      } catch (photoError) {
        console.error('Photo send error:', photoError.message);
        await bot.sendMessage(msg.chat.id,
          `${icon} *${video.title}*\n\n` +
          `👤 ${video.author.name}\n` +
          `👁️ ${formatViews(video.views)} views\n` +
          `⏱️ ${video.duration.timestamp}\n` +
          `📅 ${video.ago}` +
          statusText,
          {
            parse_mode: 'Markdown',
            reply_markup: inQueue ? 
              { inline_keyboard: [[{ text: '🔙 Back to Results', callback_data: 'back_to_search' }]] } :
              keyboards.videoConfirm(index, isDuplicate)
          }
        );
      }
    }
    
    else if (data.startsWith('confirm_video_') || data.startsWith('confirm_duplicate_')) {
      const index = parseInt(data.split('_').pop());
      const video = session.searchResults[index];
      
      if (!video) {
        return bot.answerCallbackQuery(query.id, { text: '❌ Video not found' });
      }
      
      if (session.selectedPages.length === 0) {
        return bot.answerCallbackQuery(query.id, { 
          text: '⚠️ Please select at least one page first!', 
          show_alert: true 
        });
      }
      
      const isDuplicate = isAlreadyProcessed(video.videoId);
      const forceDuplicate = data.startsWith('confirm_duplicate_');
      
      if (isDuplicate && !forceDuplicate) {
        return bot.answerCallbackQuery(query.id, { 
          text: '⚠️ This is a duplicate! Use "Add Anyway" to proceed.', 
          show_alert: true 
        });
      }
      
      addToQueue(msg.chat.id, video.url, session.selectedQuality, from.first_name, video.type, video.videoId, forceDuplicate, session.selectedPages);
      
      const icon = video.type === 'shorts' ? '📱' : '🎬';
      const pageNames = session.selectedPages.map(i => PAGE_NAMES[i]).join(', ');
      
      await bot.sendMessage(msg.chat.id,
        `✅ *Added to Queue!*\n\n${icon} ${video.title.substring(0, 50)}...\n\n` +
        `📄 Will post to:\n${pageNames}\n\n⏳ Processing will start soon.`,
        {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '📋 View Queue', callback_data: 'view_queue' }]] }
        }
      );
      
      if (!videoQueue.some(v => v.status === 'processing')) processQueue();
    }
    
    else if (data === 'back_to_search') {
      if (session.searchResults.length > 0) {
        const totalPages = Math.ceil(session.searchResults.length / 5);
        await bot.sendMessage(msg.chat.id,
          `🔍 *Search Results*\n\n` +
          `Query: "${session.lastSearchQuery}"\n` +
          `Found: ${session.searchResults.length} videos\n\n` +
          `Select a video:`,
          {
            parse_mode: 'Markdown',
            reply_markup: keyboards.searchResults(session.searchResults, 0, totalPages)
          }
        );
      } else {
        await bot.sendMessage(msg.chat.id, '🔍 Search cleared.', {
          reply_markup: keyboards.main()
        });
      }
    }
    
    else if (data === 'view_queue') {
      if (videoQueue.length === 0) {
        await bot.editMessageText('📭 *Queue Empty*\n\nAdd videos to get started!', {
          chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '🔍 Search', callback_data: 'search_videos' }, { text: '📹 Add URL', callback_data: 'add_video' }]] }
        });
      } else {
        let text = `📋 *Queue* (${videoQueue.length})\n\n`;
        const buttons = [];
        
        videoQueue.forEach((item, i) => {
          const status = item.status === 'processing' ? '⏳' : item.status === 'completed' ? '✅' : '⏸️';
          const icon = item.type === 'shorts' ? '📱' : '🎬';
          text += `${status} ${icon} ${i + 1}. ${(item.title || 'Processing...').substring(0, 35)}...\n`;
          buttons.push([{ text: `${icon} ${i + 1}. ${(item.title || '...').substring(0, 25)}`, callback_data: `queue_item_${i}` }]);
        });
        
        buttons.push([{ text: '🔄 Refresh', callback_data: 'view_queue' }, { text: '🔙 Back', callback_data: 'main_menu' }]);
        await bot.editMessageText(text, {
          chat_id: msg.chat.id, message_id: msg.message_id,
          parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons }
        });
      }
    }
    
    else if (data === 'analytics') {
      const uptime = Math.floor((Date.now() - analytics.startTime) / 60000);
      const avgSize = analytics.totalVideos > 0 ? (analytics.totalSize / analytics.totalVideos).toFixed(2) : 0;
      const successRate = analytics.totalVideos > 0 ? ((analytics.successfulPosts / analytics.totalVideos) * 100).toFixed(1) : 0;
      
      let pageStatsText = '';
      if (Object.keys(analytics.pageStats).length > 0) {
        pageStatsText = '\n\n📄 *Per Page Stats:*\n';
        PAGE_IDS.forEach((pageId, index) => {
          const stats = analytics.pageStats[pageId] || { success: 0, failed: 0 };
          pageStatsText += `${PAGE_NAMES[index]}: ✅${stats.success} ❌${stats.failed}\n`;
        });
      }
      
      await bot.editMessageText(`
📊 *Analytics*

🔍 Searches: ${analytics.searchesPerformed}
📹 Total: ${analytics.totalVideos}
📱 Shorts: ${analytics.shortsCount}
✅ Success: ${analytics.successfulPosts}
❌ Failed: ${analytics.failedPosts}
🔍 Duplicates Skipped: ${analytics.duplicatesSkipped}
📈 Success Rate: ${successRate}%

💾 Total Size: ${(analytics.totalSize / 1024).toFixed(2)} GB
📏 Average: ${avgSize} MB

⏱️ Uptime: ${uptime} min
📋 Queue: ${videoQueue.length}
🗂️ History: ${processedVideos.size} videos
${pageStatsText}
💾 Last Saved: ${analytics.lastSaved ? new Date(analytics.lastSaved).toLocaleString() : 'Never'}
      `, {
        chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '🔄 Refresh', callback_data: 'analytics' }, { text: '🔙 Back', callback_data: 'main_menu' }]] }
      });
    }
    
    else if (data === 'settings') {
      session.waitingForSearch = false;
      await bot.editMessageText('⚙️ *Settings*', {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: keyboards.settings()
      });
    }
    
    else if (data === 'quality_settings') {
      await bot.editMessageText(`🎬 *Quality*\n\nCurrent: ${session.selectedQuality}p`, {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: keyboards.quality(session.selectedQuality)
      });
    }
    
    else if (data.startsWith('quality_')) {
      session.selectedQuality = data.split('_')[1];
      await bot.editMessageText(`🎬 *Quality Updated*\n\n${session.selectedQuality}p ✓`, {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: keyboards.quality(session.selectedQuality)
      });
    }
    
    else if (data === 'save_data') {
      await saveProcessedVideos();
      await saveAnalytics();
      await bot.editMessageText(
        `💾 *Data Saved!*\n\n✅ History: ${processedVideos.size} videos\n✅ Analytics updated\n✅ Searches: ${analytics.searchesPerformed}\n✅ Time: ${new Date().toLocaleString()}\n\nData will persist after bot restart.`,
        {
          chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: keyboards.settings()
        }
      );
    }
    
    else if (data === 'clear_history') {
      await bot.editMessageText(
        `🧹 *Clear History*\n\nThis will clear ${processedVideos.size} processed videos from history.\n\nYou'll be able to re-add them again.\n\nAre you sure?`,
        {
          chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [
            [{ text: '✅ Yes, Clear', callback_data: 'confirm_clear_history' }],
            [{ text: '❌ Cancel', callback_data: 'settings' }]
          ]}
        }
      );
    }
    
    else if (data === 'confirm_clear_history') {
      const count = processedVideos.size;
      processedVideos.clear();
      await saveProcessedVideos();
      await bot.editMessageText(
        `✅ *History Cleared!*\n\n${count} videos removed from history.\n💾 Changes saved to file.\n\nYou can now re-add any video.`,
        {
          chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: keyboards.settings()
        }
      );
    }
    
    else if (data.startsWith('duplicate_confirm_')) {
      const videoId = data.replace('duplicate_confirm_', '');
      const pendingItem = session.pendingDuplicates.find(p => p.videoId === videoId);
      
      if (pendingItem) {
        addToQueue(msg.chat.id, pendingItem.url, session.selectedQuality, from.first_name, pendingItem.type, videoId, true);
        session.pendingDuplicates = session.pendingDuplicates.filter(p => p.videoId !== videoId);
        
        await bot.editMessageText(
          `✅ *Video Added Again!*\n\n${pendingItem.type === 'shorts' ? '📱' : '🎬'} Added to queue`,
          {
            chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '📋 View Queue', callback_data: 'view_queue' }]] }
          }
        );
        
        if (!videoQueue.some(v => v.status === 'processing')) processQueue();
      }
    }
    
    else if (data === 'duplicate_skip') {
      analytics.duplicatesSkipped++;
      await saveAnalytics();
      await bot.editMessageText(
        `⏭️ *Skipped*\n\nDuplicate video not added.`,
        {
          chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: keyboards.main()
        }
      );
    }
    
    else if (data.startsWith('download_pause_')) {
      const videoId = data.replace('download_pause_', '');
      const download = activeDownloads.get(videoId);
      if (download) {
        download.paused = true;
        await bot.answerCallbackQuery(query.id, { text: '⏸️ Download paused' });
      }
    }
    
    else if (data.startsWith('download_resume_')) {
      const videoId = data.replace('download_resume_', '');
      const download = activeDownloads.get(videoId);
      if (download) {
        download.paused = false;
        await bot.answerCallbackQuery(query.id, { text: '▶️ Download resumed' });
      }
    }
    
    else if (data.startsWith('download_cancel_')) {
      const videoId = data.replace('download_cancel_', '');
      const download = activeDownloads.get(videoId);
      if (download) {
        download.cancelled = true;
        activeDownloads.delete(videoId);
        await bot.answerCallbackQuery(query.id, { text: '❌ Download cancelled' });
      }
    }
    
    else if (data === 'help') {
      session.waitingForSearch = false;
      await bot.editMessageText(`
❓ *Help*

*Two Ways to Add Videos:*
1️⃣ 🔍 Search in bot
   - Click "Search Videos"
   - Enter keywords
   - Preview with thumbnails
   - Select & add

2️⃣ 📹 Paste YouTube URL
   - Click "Add by URL"
   - Paste any YouTube link
   - Auto-detects & adds

*Supported URLs:*
🔗 youtube.com/watch?v=...
🔗 youtu.be/...
🔗 youtube.com/shorts/... 📱
🔗 m.youtube.com/watch?v=...

*Features:*
🔍 Search & preview videos
📹 Multiple video queue
📱 YouTube Shorts support
🚀 Unlimited file sizes
⏯️ Pause/Resume downloads
📊 Smart progress updates (3-10s)
🔍 Duplicate detection
💾 Persistent storage
✍️ Custom captions
🎬 Quality selection
🖼️ Auto YouTube thumbnails

*Data Storage:*
💾 Auto-saves every 5 minutes
🔄 Survives bot restarts
📁 Stored in ./data folder
🧹 Can clear history anytime

*Download Controls:*
⏸️ Pause active downloads
▶️ Resume paused downloads
❌ Cancel unwanted downloads
📊 Progress updates every 3-10s

Admin: @${ADMIN_USERNAME}
      `, {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🔙 Back', callback_data: 'main_menu' }]] }
      });
    }
    
  } catch (error) {
    console.error('Callback error:', error.message);
  }
});

// ============================================
// MESSAGE HANDLER
// ============================================

bot.on('message', async (msg) => {
  if (!msg.text || msg.text.startsWith('/') || !isAdmin(msg)) return;

  const session = getUserSession(msg.from.id);

  // Handle search query
  if (session.waitingForSearch) {
    session.waitingForSearch = false;
    
    const searchMsg = await bot.sendMessage(msg.chat.id, 
      `🔍 *Searching YouTube...*\n\nQuery: "${msg.text}"\n\n⏳ Please wait...`,
      { parse_mode: 'Markdown' }
    );

    try {
      const results = await searchYouTube(msg.text);
      
      if (results.length === 0) {
        await bot.editMessageText(
          `❌ *No Results Found*\n\nQuery: "${msg.text}"\n\nTry different keywords.`,
          {
            chat_id: msg.chat.id,
            message_id: searchMsg.message_id,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '🔍 Try Again', callback_data: 'search_videos' }]] }
          }
        );
        return;
      }

      session.searchResults = results;
      session.lastSearchQuery = msg.text;
      
      const totalPages = Math.ceil(results.length / 5);
      const shorts = results.filter(v => v.type === 'shorts').length;
      const regular = results.length - shorts;
      
      await bot.editMessageText(
        `🔍 *Search Results*\n\n` +
        `Query: "${msg.text}"\n` +
        `Found: ${results.length} videos\n` +
        `🎬 Regular: ${regular} | 📱 Shorts: ${shorts}\n\n` +
        `Select a video to preview:`,
        {
          chat_id: msg.chat.id,
          message_id: searchMsg.message_id,
          parse_mode: 'Markdown',
          reply_markup: keyboards.searchResults(results, 0, totalPages)
        }
      );
      
    } catch (error) {
      await bot.editMessageText(
        `❌ *Search Failed*\n\n${error.message}\n\nTry again later.`,
        {
          chat_id: msg.chat.id,
          message_id: searchMsg.message_id,
          parse_mode: 'Markdown',
          reply_markup: keyboards.main()
        }
      );
    }
    return;
  }

  // Handle YouTube URLs
  const matches = detectYouTubeUrl(msg.text);
  if (matches.length === 0) return;

  if (session.selectedPages.length === 0) {
    return bot.sendMessage(msg.chat.id,
      `⚠️ *No Pages Selected!*\n\nPlease select at least one page to post to.`,
      {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '📄 Select Pages', callback_data: 'select_pages' }]] }
      }
    );
  }

  const newVideos = [];
  const duplicates = [];
  const inQueue = [];
  
  matches.forEach(m => {
    if (isInQueue(m.videoId)) {
      inQueue.push(m);
    } else if (isAlreadyProcessed(m.videoId)) {
      duplicates.push(m);
    } else {
      newVideos.push(m);
    }
  });

  newVideos.forEach(m => {
    addToQueue(msg.chat.id, m.url, session.selectedQuality, msg.from.first_name, m.type, m.videoId, false, session.selectedPages);
  });

  if (duplicates.length > 0) {
    for (const dup of duplicates) {
      session.pendingDuplicates.push(dup);
      
      const icon = dup.type === 'shorts' ? '📱' : '🎬';
      await bot.sendMessage(msg.chat.id, 
        `⚠️ *Duplicate Detected!*\n\n${icon} This video was already posted.\n\nVideo ID: \`${dup.videoId}\`\n\nDo you want to add it again?`,
        {
          parse_mode: 'Markdown',
          reply_markup: keyboards.duplicateConfirm(dup.videoId)
        }
      );
    }
  }

  if (inQueue.length > 0) {
    const icon = inQueue[0].type === 'shorts' ? '📱' : '🎬';
    await bot.sendMessage(msg.chat.id,
      `ℹ️ *Already in Queue*\n\n${icon} ${inQueue.length} video(s) already in queue.`,
      { parse_mode: 'Markdown' }
    );
  }

  if (newVideos.length > 0) {
    const shorts = newVideos.filter(m => m.type === 'shorts').length;
    const regular = newVideos.length - shorts;
    const pageNames = session.selectedPages.map(i => PAGE_NAMES[i]).join(', ');
    
    let text = `✅ *${newVideos.length} added!*\n\n`;
    if (regular > 0) text += `🎬 Videos: ${regular}\n`;
    if (shorts > 0) text += `📱 Shorts: ${shorts}\n`;
    text += `\n📄 Will post to:\n${pageNames}`;
    
    const addedMsg = await bot.sendMessage(msg.chat.id, text + '\n\n⏳ Processing...', { parse_mode: 'Markdown' });

    setTimeout(async () => {
      try {
        await bot.editMessageText(`✅ *${newVideos.length} in queue!*\n\n📋 Check status`, {
          chat_id: msg.chat.id, message_id: addedMsg.message_id,
          parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '📋 View Queue', callback_data: 'view_queue' }]] }
        });
      } catch {}
    }, 2000);

    if (!videoQueue.some(v => v.status === 'processing')) processQueue();
  }
});

// ============================================
// QUEUE & PROCESSING
// ============================================

function addToQueue(chatId, url, quality, userName, type, videoId, forceDuplicate = false, selectedPages = [0]) {
  videoQueue.push({
    chatId, youtubeUrl: url, quality, userName, videoId,
    type: type || 'regular',
    status: 'pending',
    addedAt: Date.now(),
    title: null,
    isDuplicate: forceDuplicate,
    selectedPages: [...selectedPages] // Store which pages to post to
  });
  if (type === 'shorts') analytics.shortsCount++;
}

async function processQueue() {
  const next = videoQueue.find(v => v.status === 'pending');
  if (!next) return;

  next.status = 'processing';

  try {
    const session = getUserSession(next.chatId);
    await processVideo(next.chatId, next.youtubeUrl, next.quality, next.userName, session.customCaption, next.type, next.videoId, next.selectedPages);
    next.status = 'completed';
    
    processedVideos.add(next.videoId);
    await saveProcessedVideos();
    await saveAnalytics();
  } catch (error) {
    next.status = 'failed';
    next.error = error.message;
    await saveAnalytics();
  }

  videoQueue.splice(videoQueue.indexOf(next), 1);
  setTimeout(processQueue, 2000);
}

async function processVideo(chatId, url, quality, userName, customCaption, type, videoId, selectedPages = [0]) {
  const icon = type === 'shorts' ? '📱' : '🎬';
  let progressMsg;
  
  try {
    // Initial message
    const pageNames = selectedPages.map(i => PAGE_NAMES[i]).join(', ');
    progressMsg = await bot.sendMessage(chatId, 
      `⏳ *Fetching ${type === 'shorts' ? 'Short' : 'Video'} Info...*\n\n${icon} Please wait...\n\n📄 Pages: ${pageNames}`,
      { parse_mode: 'Markdown', reply_markup: keyboards.downloadControl(videoId, false) }
    );

    // Fetch video info
    const response = await axios.get(`${YOUTUBE_API_BASE}?url=${encodeURIComponent(url)}`);
    if (!response.data.status) throw new Error('Video not found');

    const videoData = response.data.data;
    const title = videoData.metadata.title;
    const fileSize = videoData.download.size;
    const fileSizeMB = (fileSize / (1024 * 1024)).toFixed(2);
    const thumbnailUrl = videoData.metadata.thumbnail || videoData.metadata.image;

    analytics.totalVideos++;
    analytics.totalSize += parseFloat(fileSizeMB);

    const queueItem = videoQueue.find(v => v.videoId === videoId);
    if (queueItem) queueItem.title = title;

    // Download thumbnail
    let thumbnailBuffer = null;
    if (thumbnailUrl) {
      await bot.editMessageText(
        `🖼️ *Downloading Thumbnail...*\n\n${title.substring(0, 45)}...\n\n` +
        `📦 Video Size: ${formatBytes(fileSize)}`,
        {
          chat_id: chatId, 
          message_id: progressMsg.message_id, 
          parse_mode: 'Markdown'
        }
      );
      
      thumbnailBuffer = await downloadThumbnail(thumbnailUrl);
    }

    // Download with progress tracking
    await bot.editMessageText(
      `${icon} *Downloading Video*\n\n${title.substring(0, 45)}...\n\n` +
      `📦 Size: ${formatBytes(fileSize)}\n` +
      `${thumbnailBuffer ? '🖼️ Thumbnail: Ready\n' : ''}` +
      `📊 Progress: 0%\n${getProgressBar(0)}\n` +
      `⚡ Speed: Initializing...`,
      {
        chat_id: chatId, 
        message_id: progressMsg.message_id, 
        parse_mode: 'Markdown',
        reply_markup: keyboards.downloadControl(videoId, false)
      }
    );

    const downloadState = {
      paused: false,
      cancelled: false,
      downloadedBytes: 0,
      totalBytes: fileSize,
      startTime: Date.now(),
      lastUpdate: Date.now(),
      chunks: []
    };
    
    activeDownloads.set(videoId, downloadState);

    const videoResponse = await axios.get(videoData.download.url, {
      responseType: 'stream',
      timeout: 0,
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    });

    const totalBytes = parseInt(videoResponse.headers['content-length'] || fileSize);
    downloadState.totalBytes = totalBytes;

    let lastPercent = -1;
    let lastUpdateTime = Date.now();
    const MIN_UPDATE_INTERVAL = 3000;

    videoResponse.data.on('data', async (chunk) => {
      while (downloadState.paused && !downloadState.cancelled) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      if (downloadState.cancelled) {
        videoResponse.data.destroy();
        throw new Error('Download cancelled by user');
      }

      downloadState.chunks.push(chunk);
      downloadState.downloadedBytes += chunk.length;

      const percent = Math.floor((downloadState.downloadedBytes / totalBytes) * 100);
      const now = Date.now();
      const elapsed = (now - downloadState.startTime) / 1000;
      const speed = downloadState.downloadedBytes / elapsed;
      const timeSinceLastUpdate = now - lastUpdateTime;

      const shouldUpdate = (percent !== lastPercent && timeSinceLastUpdate >= MIN_UPDATE_INTERVAL) || 
                          timeSinceLastUpdate >= 10000;

      if (shouldUpdate) {
        lastPercent = percent;
        lastUpdateTime = now;

        const eta = speed > 0 ? ((totalBytes - downloadState.downloadedBytes) / speed) : 0;
        const etaMin = Math.floor(eta / 60);
        const etaSec = Math.floor(eta % 60);

        try {
          await bot.editMessageText(
            `${icon} *Downloading Video*\n\n${title.substring(0, 45)}...\n\n` +
            `📦 Size: ${formatBytes(totalBytes)}\n` +
            `${thumbnailBuffer ? '🖼️ Thumbnail: Ready\n' : ''}` +
            `📥 Downloaded: ${formatBytes(downloadState.downloadedBytes)}\n` +
            `📊 Progress: ${percent}%\n${getProgressBar(percent)}\n` +
            `⚡ Speed: ${formatSpeed(speed)}\n` +
            `⏱️ ETA: ${etaMin}m ${etaSec}s`,
            {
              chat_id: chatId,
              message_id: progressMsg.message_id,
              parse_mode: 'Markdown',
              reply_markup: keyboards.downloadControl(videoId, downloadState.paused)
            }
          );
        } catch (err) {
          if (err.response?.body?.error_code === 429) {
            console.log('⚠️ Rate limited, skipping update');
          }
        }
      }
    });

    const videoBuffer = await new Promise((resolve, reject) => {
      videoResponse.data.on('end', () => {
        if (downloadState.cancelled) {
          reject(new Error('Download cancelled'));
        } else {
          resolve(Buffer.concat(downloadState.chunks));
        }
      });
      videoResponse.data.on('error', reject);
    });

    activeDownloads.delete(videoId);

    // Upload to Facebook - Multiple Pages
    const pageNames = selectedPages.map(i => PAGE_NAMES[i]).join(', ');
    await bot.editMessageText(
      `${icon} *Uploading to Facebook...*\n\n${title.substring(0, 45)}...\n\n` +
      `📦 Size: ${formatBytes(totalBytes)}\n` +
      `${thumbnailBuffer ? '🖼️ Thumbnail: Included\n' : ''}` +
      `📄 Pages: ${pageNames}\n` +
      `📊 Progress: 0%\n${getProgressBar(0)}`,
      {
        chat_id: chatId,
        message_id: progressMsg.message_id,
        parse_mode: 'Markdown'
      }
    );

    const caption = customCaption || title;
    
    // Upload to each selected page
    const uploadResults = [];
    for (let i = 0; i < selectedPages.length; i++) {
      const pageIndex = selectedPages[i];
      const pageId = PAGE_IDS[pageIndex];
      const pageName = PAGE_NAMES[pageIndex];
      
      await bot.editMessageText(
        `${icon} *Uploading to ${pageName}...*\n\n${title.substring(0, 45)}...\n\n` +
        `📦 Size: ${formatBytes(totalBytes)}\n` +
        `${thumbnailBuffer ? '🖼️ Thumbnail: Included\n' : ''}` +
        `📄 Progress: ${i + 1}/${selectedPages.length} pages\n` +
        `📊 ${Math.floor(((i) / selectedPages.length) * 100)}%\n${getProgressBar(Math.floor(((i) / selectedPages.length) * 100))}`,
        {
          chat_id: chatId,
          message_id: progressMsg.message_id,
          parse_mode: 'Markdown'
        }
      );
      
      try {
        const result = await uploadVideoToFacebook(
          Buffer.from(videoBuffer), 
          caption, 
          chatId, 
          progressMsg.message_id, 
          title, 
          icon, 
          totalBytes, 
          thumbnailBuffer,
          pageId,
          pageName
        );
        
        uploadResults.push({ page: pageName, success: true, data: result });
        
        // Update page stats
        if (!analytics.pageStats[pageId]) {
          analytics.pageStats[pageId] = { success: 0, failed: 0 };
        }
        analytics.pageStats[pageId].success++;
        
      } catch (error) {
        console.error(`❌ Failed to upload to ${pageName}:`, error.message);
        uploadResults.push({ page: pageName, success: false, error: error.message });
        
        // Update page stats
        if (!analytics.pageStats[pageId]) {
          analytics.pageStats[pageId] = { success: 0, failed: 0 };
        }
        analytics.pageStats[pageId].failed++;
      }
    }

    const successCount = uploadResults.filter(r => r.success).length;
    const failedCount = uploadResults.filter(r => !r.success).length;
    
    if (successCount > 0) analytics.successfulPosts++;
    if (failedCount === selectedPages.length) analytics.failedPosts++;

    let resultText = `${successCount > 0 ? '✅' : '❌'} *Upload ${successCount > 0 ? 'Complete' : 'Failed'}!*\n\n${icon} ${title.substring(0, 50)}...\n\n` +
      `📦 Size: ${formatBytes(totalBytes)}\n` +
      `${thumbnailBuffer ? '🖼️ Thumbnail: Added\n' : ''}` +
      `🆔 Video ID: \`${videoId}\`\n\n` +
      `📄 *Results:*\n`;
    
    uploadResults.forEach(r => {
      resultText += `${r.success ? '✅' : '❌'} ${r.page}\n`;
    });
    
    resultText += `\n⏱️ Completed: ${new Date().toLocaleTimeString()}`;

    await bot.editMessageText(resultText,
      {
        chat_id: chatId,
        message_id: progressMsg.message_id,
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '📊 Analytics', callback_data: 'analytics' }, { text: '🏠 Menu', callback_data: 'main_menu' }]
          ]
        }
      }
    );

  } catch (error) {
    analytics.failedPosts++;
    console.error('Process error:', error.message);
    
    activeDownloads.delete(videoId);
    
    if (progressMsg) {
      try {
        await bot.editMessageText(
          `❌ *Error*\n\n${error.message}\n\nTry again or contact admin.`,
          {
            chat_id: chatId,
            message_id: progressMsg.message_id,
            parse_mode: 'Markdown',
            reply_markup: keyboards.main()
          }
        );
      } catch {}
    }
    throw error;
  }
}

// ============================================
// FACEBOOK UPLOAD WITH PROGRESS & THUMBNAIL
// ============================================

async function uploadVideoToFacebook(videoBuffer, title, chatId, messageId, videoTitle, icon, totalBytes, thumbnailBuffer = null, pageId, pageName) {
  try {
    console.log(`🚀 Uploading to Facebook page: ${pageName} (${pageId})...`);
    
    // Initialize upload session
    const initResponse = await axios.post(`https://graph.facebook.com/v18.0/${pageId}/videos`, null, {
      params: { 
        upload_phase: 'start', 
        access_token: PAGE_ACCESS_TOKEN, 
        file_size: videoBuffer.length 
      }
    });

    const uploadSessionId = initResponse.data.upload_session_id;
    console.log('✅ Session:', uploadSessionId);

    const chunkSize = 5 * 1024 * 1024; // 5MB chunks
    let offset = 0;
    let lastPercent = -1;
    let lastUploadUpdate = Date.now();
    const MIN_UPLOAD_UPDATE_INTERVAL = 5000; // Slower updates for multi-page

    while (offset < videoBuffer.length) {
      const chunk = videoBuffer.slice(offset, Math.min(offset + chunkSize, videoBuffer.length));
      const percent = Math.floor((offset / videoBuffer.length) * 100);
      const now = Date.now();
      const timeSinceLastUpdate = now - lastUploadUpdate;

      const shouldUpdate = (percent !== lastPercent && timeSinceLastUpdate >= MIN_UPLOAD_UPDATE_INTERVAL) || 
                          timeSinceLastUpdate >= 15000;

      if (shouldUpdate) {
        lastPercent = percent;
        lastUploadUpdate = now;
        
        try {
          await bot.editMessageText(
            `${icon} *Uploading to ${pageName}*\n\n${videoTitle.substring(0, 45)}...\n\n` +
            `📦 Size: ${formatBytes(totalBytes)}\n` +
            `${thumbnailBuffer ? '🖼️ Thumbnail: Ready\n' : ''}` +
            `📤 Uploaded: ${formatBytes(offset)}\n` +
            `📊 Progress: ${percent}%\n${getProgressBar(percent)}`,
            {
              chat_id: chatId,
              message_id: messageId,
              parse_mode: 'Markdown'
            }
          );
        } catch (err) {
          if (err.response?.body?.error_code === 429) {
            console.log('⚠️ Upload: Rate limited, skipping update');
          }
        }
      }

      const formData = new FormData();
      formData.append('access_token', PAGE_ACCESS_TOKEN);
      formData.append('upload_phase', 'transfer');
      formData.append('upload_session_id', uploadSessionId);
      formData.append('start_offset', offset);
      formData.append('video_file_chunk', chunk, { 
        filename: 'video.mp4', 
        contentType: 'video/mp4' 
      });

      console.log(`📤 ${pageName}: ${percent}%`);

      await axios.post(`https://graph.facebook.com/v18.0/${pageId}/videos`, formData, {
        headers: formData.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 0
      });

      offset += chunk.length;
    }

    // Finalize upload
    const finishParams = {
      upload_phase: 'finish',
      access_token: PAGE_ACCESS_TOKEN,
      upload_session_id: uploadSessionId,
      title: title,
      description: title
    };

    // Upload thumbnail if available
    if (thumbnailBuffer) {
      console.log(`🖼️ Uploading thumbnail to ${pageName}...`);
      
      try {
        const thumbnailFormData = new FormData();
        for (const [key, value] of Object.entries(finishParams)) {
          thumbnailFormData.append(key, value);
        }
        thumbnailFormData.append('thumb', thumbnailBuffer, {
          filename: 'thumbnail.jpg',
          contentType: 'image/jpeg'
        });

        const finishResponse = await axios.post(
          `https://graph.facebook.com/v18.0/${pageId}/videos`, 
          thumbnailFormData,
          {
            headers: thumbnailFormData.getHeaders(),
            maxContentLength: Infinity,
            maxBodyLength: Infinity
          }
        );

        console.log(`✅ Published with thumbnail to ${pageName}! ID:`, finishResponse.data.id);
        return finishResponse.data;
        
      } catch (thumbError) {
        console.error(`⚠️ Thumbnail upload failed for ${pageName}:`, thumbError.message);
        
        const finishResponse = await axios.post(`https://graph.facebook.com/v18.0/${pageId}/videos`, null, {
          params: finishParams
        });

        console.log(`✅ Published without thumbnail to ${pageName}! ID:`, finishResponse.data.id);
        return finishResponse.data;
      }
    } else {
      const finishResponse = await axios.post(`https://graph.facebook.com/v18.0/${pageId}/videos`, null, {
        params: finishParams
      });

      console.log(`✅ Published to ${pageName}! ID:`, finishResponse.data.id);
      return finishResponse.data;
    }

  } catch (error) {
    console.error(`❌ Facebook error for ${pageName}:`, error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || 'Facebook upload failed');
  }
}

// ============================================
// STARTUP & INITIALIZATION
// ============================================

async function initializeBot() {
  console.log('🚀 Initializing bot...');
  
  await ensureDataDirectory();
  await loadProcessedVideos();
  await loadAnalytics();
  
  console.log('✅ Bot ready! ULTIMATE MODE with Multi-Page, Search & Thumbnail support enabled 📄🔍🖼️🚀');
  console.log(`📊 Loaded: ${processedVideos.size} videos, ${analytics.totalVideos} total processed`);
  console.log(`🔍 Total searches performed: ${analytics.searchesPerformed}`);
  console.log(`📄 Configured pages: ${PAGE_IDS.length}`);
  PAGE_IDS.forEach((id, i) => {
    console.log(`   ${i + 1}. ${PAGE_NAMES[i]} (ID: ${id})`);
  });
}

initializeBot().catch(error => {
  console.error('❌ Initialization error:', error);
});

// ============================================
// ERROR HANDLING
// ============================================

bot.on('polling_error', (error) => console.error('Polling:', error.message));
process.on('uncaughtException', (error) => console.error('Exception:', error));
process.on('unhandledRejection', (error) => console.error('Rejection:', error));

// ============================================
// GRACEFUL SHUTDOWN
// ============================================

async function gracefulShutdown() {
  console.log('\n🛑 Shutting down gracefully...');
  
  console.log('💾 Saving data...');
  await saveProcessedVideos();
  await saveAnalytics();
  
  console.log('✅ Data saved successfully');
  console.log('👋 Goodbye!');
  
  bot.stopPolling();
  process.exit(0);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

console.log('✅ Bot script loaded - ULTIMATE MODE with MULTI-PAGE, SEARCH & THUMBNAIL SUPPORT 📄🔍🖼️🚀');
