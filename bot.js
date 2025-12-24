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
const PAGE_ID = process.env.PAGE_ID;
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'kalindu_gaweshana';

// Data file paths
const DATA_DIR = path.join(__dirname, 'data');
const HISTORY_FILE = path.join(DATA_DIR, 'processed_videos.json');
const ANALYTICS_FILE = path.join(DATA_DIR, 'analytics.json');

if (!TELEGRAM_TOKEN || !PAGE_ACCESS_TOKEN || !PAGE_ID) {
  console.error('❌ Missing required environment variables!');
  process.exit(1);
}

const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });
const YOUTUBE_API_BASE = 'https://youtube-apis.vercel.app/api/ytmp4';

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
  startTime: Date.now(),
  lastSaved: null
};
const scheduledPosts = [];
const userSessions = new Map();
const activeDownloads = new Map(); // Track active downloads for pause/resume

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
      pendingDuplicates: []
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

// ============================================
// KEYBOARDS
// ============================================

const keyboards = {
  main: () => ({
    inline_keyboard: [
      [{ text: '📹 Add Video', callback_data: 'add_video' }, { text: '📋 View Queue', callback_data: 'view_queue' }],
      [{ text: '⚙️ Settings', callback_data: 'settings' }, { text: '📊 Analytics', callback_data: 'analytics' }],
      [{ text: '⏰ Schedule', callback_data: 'schedule_menu' }, { text: '❓ Help', callback_data: 'help' }]
    ]
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

🤖 *YouTube to Facebook Bot - UNLIMITED*

✅ Regular YouTube videos
✅ YouTube Shorts 📱
✅ No size limits 🚀
✅ Pause/Resume downloads ⏯️
✅ Smart progress (3-10s) 📊
✅ Auto posting to Facebook
✅ Duplicate detection 🔍
✅ Persistent history 💾

*Features:*
💾 Data saved automatically
🔄 Survives bot restarts
🔍 Smart duplicate detection
⏯️ Pause/Resume downloads
📊 Progress updates every 3-10s
🚀 Unlimited file sizes

*Supported URLs:*
🔗 youtube.com/watch?v=...
🔗 youtu.be/...
🔗 youtube.com/shorts/... 📱
🔗 m.youtube.com/watch?v=...
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
    
    else if (data === 'add_video') {
      await bot.editMessageText(
        '📹 *Add Video - UNLIMITED*\n\n✅ No size limits\n✅ Shorts & Regular videos 📱\n✅ Pause/Resume support ⏯️\n✅ Smart progress (3-10s)\n🔍 Duplicate detection enabled\n\nSend YouTube links:',
        { chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '🔙 Back', callback_data: 'main_menu' }]] }
        }
      );
    }
    
    else if (data === 'view_queue') {
      if (videoQueue.length === 0) {
        await bot.editMessageText('📭 *Queue Empty*\n\nAdd videos to get started!', {
          chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '📹 Add Video', callback_data: 'add_video' }]] }
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
      
      await bot.editMessageText(`
📊 *Analytics*

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
⏰ Scheduled: ${scheduledPosts.length}
🗂️ History: ${processedVideos.size} videos

💾 Last Saved: ${analytics.lastSaved ? new Date(analytics.lastSaved).toLocaleString() : 'Never'}
      `, {
        chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '🔄 Refresh', callback_data: 'analytics' }, { text: '🔙 Back', callback_data: 'main_menu' }]] }
      });
    }
    
    else if (data === 'settings') {
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
    
    else if (data === 'save_data') {
      await saveProcessedVideos();
      await saveAnalytics();
      await bot.editMessageText(
        `💾 *Data Saved!*\n\n✅ History: ${processedVideos.size} videos\n✅ Analytics updated\n✅ Time: ${new Date().toLocaleString()}\n\nData will persist after bot restart.`,
        {
          chat_id: msg.chat.id, message_id: msg.message_id, parse_mode: 'Markdown',
          reply_markup: keyboards.settings()
        }
      );
    }
    
    else if (data === 'data_info') {
      const historyExists = await fs.access(HISTORY_FILE).then(() => true).catch(() => false);
      const analyticsExists = await fs.access(ANALYTICS_FILE).then(() => true).catch(() => false);
      
      let historySize = 0, analyticsSize = 0;
      try {
        if (historyExists) {
          const stats = await fs.stat(HISTORY_FILE);
          historySize = (stats.size / 1024).toFixed(2);
        }
        if (analyticsExists) {
          const stats = await fs.stat(ANALYTICS_FILE);
          analyticsSize = (stats.size / 1024).toFixed(2);
        }
      } catch {}
      
      await bot.editMessageText(
        `📊 *Data Information*\n\n📁 Storage Location:\n\`${DATA_DIR}\`\n\n` +
        `📂 History File:\n${historyExists ? '✅ Exists' : '❌ Not found'}\n` +
        `Size: ${historySize} KB\nVideos: ${processedVideos.size}\n\n` +
        `📂 Analytics File:\n${analyticsExists ? '✅ Exists' : '❌ Not found'}\n` +
        `Size: ${analyticsSize} KB\n\n` +
        `💾 Auto-save: Every 5 minutes\n` +
        `🔄 Last saved: ${analytics.lastSaved ? new Date(analytics.lastSaved).toLocaleString() : 'Never'}`,
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
    
    else if (data.startsWith('quality_')) {
      session.selectedQuality = data.split('_')[1];
      await bot.editMessageText(`🎬 *Quality Updated*\n\n${session.selectedQuality}p ✓`, {
        chat_id: msg.chat.id, message_id: msg.message_id,
        parse_mode: 'Markdown', reply_markup: keyboards.quality(session.selectedQuality)
      });
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
      await bot.editMessageText(`
❓ *Help*

*Supported URLs:*
🔗 youtube.com/watch?v=...
🔗 youtu.be/...
🔗 youtube.com/shorts/... 📱
🔗 m.youtube.com/watch?v=...

*Features:*
📹 Multiple video queue
📱 YouTube Shorts support
🚀 Unlimited file sizes
⏯️ Pause/Resume downloads
📊 Smart progress updates (3-10s)
🔍 Duplicate detection
💾 Persistent storage
⏰ Schedule posts
✍️ Custom captions
🎬 Quality selection

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

  const matches = detectYouTubeUrl(msg.text);
  if (matches.length === 0) return;

  const session = getUserSession(msg.from.id);
  
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
    addToQueue(msg.chat.id, m.url, session.selectedQuality, msg.from.first_name, m.type, m.videoId);
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
    
    let text = `✅ *${newVideos.length} added!*\n\n`;
    if (regular > 0) text += `🎬 Videos: ${regular}\n`;
    if (shorts > 0) text += `📱 Shorts: ${shorts}\n`;
    
    const addedMsg = await bot.sendMessage(msg.chat.id, text + '\n⏳ Processing...', { parse_mode: 'Markdown' });

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

function addToQueue(chatId, url, quality, userName, type, videoId, forceDuplicate = false) {
  videoQueue.push({
    chatId, youtubeUrl: url, quality, userName, videoId,
    type: type || 'regular',
    status: 'pending',
    addedAt: Date.now(),
    title: null,
    isDuplicate: forceDuplicate
  });
  if (type === 'shorts') analytics.shortsCount++;
}

async function processQueue() {
  const next = videoQueue.find(v => v.status === 'pending');
  if (!next) return;

  next.status = 'processing';

  try {
    const session = getUserSession(next.chatId);
    await processVideo(next.chatId, next.youtubeUrl, next.quality, next.userName, session.customCaption, next.type, next.videoId);
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

async function processVideo(chatId, url, quality, userName, customCaption, type, videoId) {
  const icon = type === 'shorts' ? '📱' : '🎬';
  let progressMsg;
  
  try {
    // Initial message
    progressMsg = await bot.sendMessage(chatId, 
      `⏳ *Fetching ${type === 'shorts' ? 'Short' : 'Video'} Info...*\n\n${icon} Please wait...`,
      { parse_mode: 'Markdown', reply_markup: keyboards.downloadControl(videoId, false) }
    );

    // Fetch video info
    const response = await axios.get(`${YOUTUBE_API_BASE}?url=${encodeURIComponent(url)}`);
    if (!response.data.status) throw new Error('Video not found');

    const videoData = response.data.data;
    const title = videoData.metadata.title;
    const fileSize = videoData.download.size;
    const fileSizeMB = (fileSize / (1024 * 1024)).toFixed(2);

    analytics.totalVideos++;
    analytics.totalSize += parseFloat(fileSizeMB);

    const queueItem = videoQueue.find(v => v.videoId === videoId);
    if (queueItem) queueItem.title = title;

    // Download with progress tracking
    await bot.editMessageText(
      `${icon} *Downloading*\n\n${title.substring(0, 45)}...\n\n` +
      `📦 Size: ${formatBytes(fileSize)}\n` +
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
      timeout: 0, // No timeout
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    });

    const totalBytes = parseInt(videoResponse.headers['content-length'] || fileSize);
    downloadState.totalBytes = totalBytes;

    let lastPercent = -1;
    let lastUpdateTime = Date.now();
    const MIN_UPDATE_INTERVAL = 3000; // 3 seconds minimum between updates

    videoResponse.data.on('data', async (chunk) => {
      // Check if paused
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

      // Update only if:
      // 1. Percent changed AND at least 3 seconds passed
      // 2. OR 10 seconds passed (force update)
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
            `${icon} *Downloading*\n\n${title.substring(0, 45)}...\n\n` +
            `📦 Size: ${formatBytes(totalBytes)}\n` +
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
          // Ignore edit errors (Telegram rate limit)
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

    // Upload to Facebook
    await bot.editMessageText(
      `${icon} *Uploading to Facebook...*\n\n${title.substring(0, 45)}...\n\n` +
      `📦 Size: ${formatBytes(totalBytes)}\n` +
      `📊 Progress: 0%\n${getProgressBar(0)}`,
      {
        chat_id: chatId,
        message_id: progressMsg.message_id,
        parse_mode: 'Markdown'
      }
    );

    const caption = customCaption || title;
    await uploadVideoToFacebook(Buffer.from(videoBuffer), caption, chatId, progressMsg.message_id, title, icon, totalBytes);

    analytics.successfulPosts++;

    await bot.editMessageText(
      `✅ *Posted Successfully!*\n\n${icon} ${title.substring(0, 50)}...\n\n` +
      `📦 Size: ${formatBytes(totalBytes)}\n` +
      `🆔 Video ID: \`${videoId}\`\n` +
      `⏱️ Completed: ${new Date().toLocaleTimeString()}`,
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
// FACEBOOK UPLOAD WITH PROGRESS
// ============================================

async function uploadVideoToFacebook(videoBuffer, title, chatId, messageId, videoTitle, icon, totalBytes) {
  try {
    console.log('🚀 Uploading to Facebook...');
    
    // Initialize upload session
    const initResponse = await axios.post(`https://graph.facebook.com/v18.0/${PAGE_ID}/videos`, null, {
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
    const MIN_UPLOAD_UPDATE_INTERVAL = 3000; // 3 seconds between updates

    while (offset < videoBuffer.length) {
      const chunk = videoBuffer.slice(offset, Math.min(offset + chunkSize, videoBuffer.length));
      const percent = Math.floor((offset / videoBuffer.length) * 100);
      const now = Date.now();
      const timeSinceLastUpdate = now - lastUploadUpdate;

      // Update only if:
      // 1. Percent changed AND at least 3 seconds passed
      // 2. OR 10 seconds passed (force update)
      const shouldUpdate = (percent !== lastPercent && timeSinceLastUpdate >= MIN_UPLOAD_UPDATE_INTERVAL) || 
                          timeSinceLastUpdate >= 10000;

      if (shouldUpdate) {
        lastPercent = percent;
        lastUploadUpdate = now;
        
        try {
          await bot.editMessageText(
            `${icon} *Uploading to Facebook*\n\n${videoTitle.substring(0, 45)}...\n\n` +
            `📦 Size: ${formatBytes(totalBytes)}\n` +
            `📤 Uploaded: ${formatBytes(offset)}\n` +
            `📊 Progress: ${percent}%\n${getProgressBar(percent)}`,
            {
              chat_id: chatId,
              message_id: messageId,
              parse_mode: 'Markdown'
            }
          );
        } catch (err) {
          // Ignore edit errors (Telegram rate limit)
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

      console.log(`📤 Uploading: ${percent}%`);

      await axios.post(`https://graph.facebook.com/v18.0/${PAGE_ID}/videos`, formData, {
        headers: formData.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 0 // No timeout
      });

      offset += chunk.length;
    }

    // Finalize upload
    await bot.editMessageText(
      `${icon} *Finalizing Upload...*\n\n${videoTitle.substring(0, 45)}...\n\n` +
      `📦 Size: ${formatBytes(totalBytes)}\n` +
      `📊 Progress: 100%\n${getProgressBar(100)}\n\n` +
      `⏳ Processing on Facebook...`,
      {
        chat_id: chatId,
        message_id: messageId,
        parse_mode: 'Markdown'
      }
    );

    const finishResponse = await axios.post(`https://graph.facebook.com/v18.0/${PAGE_ID}/videos`, null, {
      params: {
        upload_phase: 'finish',
        access_token: PAGE_ACCESS_TOKEN,
        upload_session_id: uploadSessionId,
        title: title,
        description: title
      }
    });

    console.log('✅ Published! ID:', finishResponse.data.id);
    return finishResponse.data;

  } catch (error) {
    console.error('❌ Facebook error:', error.response?.data || error.message);
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
  
  console.log('✅ Bot ready! Unlimited mode enabled 🚀');
  console.log(`📊 Loaded: ${processedVideos.size} videos, ${analytics.totalVideos} total processed`);
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

console.log('✅ Bot script loaded - UNLIMITED MODE 🚀');
