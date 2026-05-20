// Slackランキング - オンデマンド集計（DB保存なし）
const CHAT_CHANNEL_ID       = process.env.RANKING_CHAT_CHANNEL_ID || '';
const REPORT_IN_CHANNEL_ID  = process.env.RANKING_REPORT_IN_CHANNEL_ID || '';
const REPORT_OUT_CHANNEL_ID = process.env.RANKING_REPORT_OUT_CHANNEL_ID || '';
const REPORT_BOT_ID         = process.env.RANKING_REPORT_BOT_ID || '';
const REPORT_APP_ID         = process.env.RANKING_REPORT_APP_ID || '';
const REPORT_TEXT_KEYWORD   = process.env.RANKING_REPORT_TEXT_KEYWORD || '';
const PARENT_LOOKBACK_DAYS  = Number(process.env.RANKING_PARENT_LOOKBACK_DAYS || 2);

const CHAT_CH      = { id: CHAT_CHANNEL_ID,       name: 'all-雑談',    category: 'chat' };
const REPORT_IN_CH = { id: REPORT_IN_CHANNEL_ID,  name: 'all-出勤日報', category: 'report_in_reply' };
const REPORT_OUT_CH= { id: REPORT_OUT_CHANNEL_ID, name: 'all-退勤日報', category: 'report_out_reply' };

function needsChannel(category, ch) {
  if (ch.category === 'chat')
    return ['all', 'chat'].includes(category);
  if (ch.category === 'report_in_reply')
    return ['all', 'report_in_reply', 'report_reply'].includes(category);
  if (ch.category === 'report_out_reply')
    return ['all', 'report_out_reply', 'report_reply'].includes(category);
  return true;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function toSlackTs(date) { return String(Math.floor(date.getTime() / 1000)); }
function inRange(ts, from, to) {
  const n = Number(ts);
  return n >= from.getTime() / 1000 && n <= to.getTime() / 1000;
}
function isHuman(msg) {
  if (!msg?.user) return false;
  if (msg.bot_id || msg.app_id) return false;
  if (['bot_message','channel_join','channel_leave'].includes(msg.subtype)) return false;
  return true;
}
// daily-report-watcherの投稿を識別するキーワード（除外対象）
const EXCLUDE_KEYWORDS = ['未提出者をお知らせ', '未提出者はいません'];

function isReportParent(msg) {
  if (!msg) return false;
  const isBot = msg.subtype === 'bot_message' || !!msg.bot_id || !!msg.app_id;
  if (!isBot) return false;
  if (REPORT_BOT_ID && msg.bot_id !== REPORT_BOT_ID) return false;
  if (REPORT_APP_ID && msg.app_id !== REPORT_APP_ID) return false;
  if (REPORT_TEXT_KEYWORD && !(msg.text || '').includes(REPORT_TEXT_KEYWORD)) return false;
  // daily-report-watcher の通知投稿は除外
  const text = msg.text || '';
  if (EXCLUDE_KEYWORDS.some(kw => text.includes(kw))) return false;
  return true;
}

async function fetchHistory(client, channelId, oldest, latest) {
  let cursor, all = [];
  do {
    const res = await client.conversations.history({ channel: channelId, oldest, latest, inclusive: true, limit: 200, cursor });
    all.push(...(res.messages || []));
    cursor = res.response_metadata?.next_cursor;
  } while (cursor);
  return all;
}

async function fetchReplies(client, channelId, ts) {
  try {
    let cursor, all = [];
    do {
      const res = await client.conversations.replies({ channel: channelId, ts, limit: 200, cursor });
      all.push(...(res.messages || []));
      cursor = res.response_metadata?.next_cursor;
    } while (cursor);
    return all;
  } catch (e) {
    console.error('[ranking] replies error', e?.data?.error || e?.message);
    await sleep(1000);
    return [];
  }
}

async function fetchUserNames(botClient) {
  let cursor, members = [];
  do {
    const res = await botClient.users.list({ limit: 200, cursor });
    members.push(...(res.members || []));
    cursor = res.response_metadata?.next_cursor;
  } while (cursor);
  const map = {};
  for (const u of members) map[u.id] = u.profile?.display_name || u.real_name || u.name || u.id;
  return map;
}

// 同一スレッドへの複数投稿も1件ずつカウント（常にAPIで取得）
async function processReplies(client, channelId, messages, from, to, counter, key) {
  const replyMessages = messages.filter(m => m.reply_count && m.ts);
  for (const msg of replyMessages) {
    const replies = await fetchReplies(client, channelId, msg.ts);
    for (const r of replies.slice(1)) { // 先頭は親メッセージなのでスキップ
      if (isHuman(r) && inRange(r.ts, from, to)) {
        if (!counter[r.user]) counter[r.user] = { chat: 0, report_in: 0, report_out: 0 };
        counter[r.user][key || 'chat']++;
      }
    }
    await sleep(300);
  }
}

async function collectChannel(botClient, replyClient, channelConfig, from, to, counter, onProgress) {
  if (!channelConfig.id) return;
  const oldest = toSlackTs(channelConfig.category === 'chat' ? from : (() => {
    const d = new Date(from); d.setDate(d.getDate() - PARENT_LOOKBACK_DAYS); return d;
  })());
  const messages = await fetchHistory(botClient, channelConfig.id, oldest, toSlackTs(to));
  onProgress?.({ channel: channelConfig.name, phase: 'fetched', total: messages.length });

  if (channelConfig.category === 'chat') {
    for (const msg of messages) {
      if (isHuman(msg) && inRange(msg.ts, from, to)) {
        if (!counter[msg.user]) counter[msg.user] = { chat: 0, report_in: 0, report_out: 0 };
        counter[msg.user].chat++;
      }
    }
    // bot投稿スレッドへの返信は除外 → 人間の投稿のみ対象
    const humanThreads = messages.filter(m => isHuman(m));
    await processReplies(replyClient, channelConfig.id, humanThreads, from, to, counter, 'chat');
  } else {
    const parents = messages.filter(m => isReportParent(m));
    const key = channelConfig.category === 'report_in_reply' ? 'report_in' : 'report_out';
    await processReplies(replyClient, channelConfig.id, parents, from, to, counter, key);
  }
  onProgress?.({ channel: channelConfig.name, phase: 'done', total: messages.length });
  console.log(`[ranking] ${channelConfig.name}: done (${messages.length} msgs)`);
}

async function collectAndAggregate(botClient, replyClient, from, to, category, limit, onProgress) {
  const nameMap = await fetchUserNames(botClient);
  onProgress?.({ step: 'users_loaded' });

  const counter = {};
  const channels = [CHAT_CH, REPORT_IN_CH, REPORT_OUT_CH].filter(ch => needsChannel(category, ch) && ch.id);

  // 3チャンネルを並列取得（reply_users活用でAPI呼び出しが少ないので安全）
  await Promise.all(channels.map(ch =>
    collectChannel(botClient, replyClient, ch, from, to, counter, onProgress)
  ));

  let ranking = Object.entries(counter).map(([userId, c]) => ({
    user_id: userId,
    user_name: nameMap[userId] || userId,
    chat_count:       c.chat,
    report_in_count:  c.report_in,
    report_out_count: c.report_out,
    total: c.chat + c.report_in + c.report_out,
  }));

  const sortFn = {
    'all':              (a, b) => b.total - a.total,
    'chat':             (a, b) => b.chat_count - a.chat_count,
    'report_in_reply':  (a, b) => b.report_in_count - a.report_in_count,
    'report_out_reply': (a, b) => b.report_out_count - a.report_out_count,
    'report_reply':     (a, b) => (b.report_in_count + b.report_out_count) - (a.report_in_count + a.report_out_count),
  }[category] || ((a, b) => b.total - a.total);

  ranking = ranking.sort(sortFn);
  if (limit > 0) ranking = ranking.slice(0, limit);
  return ranking.map((r, i) => ({ ...r, rank: i + 1 }));
}

// ─── ジョブ管理（メモリ内） ──────────────────────────────────────
const jobs = new Map();

function registerRankingApi({ expressApp, authWithRole, slackClient }) {
  const userToken = process.env.RANKING_SLACK_USER_TOKEN;
  const replyClient = userToken
    ? new (require('@slack/web-api').WebClient)(userToken)
    : slackClient;

  // POST /api/dashboard/ranking/start
  expressApp.post('/api/dashboard/ranking/start', authWithRole, async (req, res) => {
    const { from, to, category = 'all', limit = '20' } = req.body || {};
    if (!from || !to) return res.status(400).json({ error: 'from/to required' });
    if (!CHAT_CHANNEL_ID && !REPORT_IN_CHANNEL_ID && !REPORT_OUT_CHANNEL_ID)
      return res.status(400).json({ error: 'channel env vars not set' });

    const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const channels = [CHAT_CH, REPORT_IN_CH, REPORT_OUT_CH]
      .filter(ch => needsChannel(category, ch) && ch.id)
      .map(ch => ch.name);

    jobs.set(jobId, {
      status: 'running', ranking: null, error: null,
      startedAt: Date.now(),
      progress: { channels, done: [], current: null },
    });

    (async () => {
      try {
        const fromDate = new Date(`${from}T00:00:00+09:00`);
        const toDate   = new Date(`${to}T23:59:59+09:00`);

        const ranking = await collectAndAggregate(
          slackClient, replyClient, fromDate, toDate, category, Number(limit),
          ({ channel, phase }) => {
            const job = jobs.get(jobId);
            if (!job) return;
            if (phase === 'fetched') job.progress.current = channel;
            if (phase === 'done') {
              job.progress.done.push(channel);
              job.progress.current = null;
            }
          }
        );
        const job = jobs.get(jobId);
        if (job) { job.status = 'done'; job.ranking = ranking; }
      } catch (e) {
        console.error(`[ranking] job ${jobId} error:`, e);
        const job = jobs.get(jobId);
        if (job) { job.status = 'error'; job.error = e.message; }
      }
    })();

    res.json({ jobId, channels });
  });

  // GET /api/dashboard/ranking/status/:jobId
  expressApp.get('/api/dashboard/ranking/status/:jobId', authWithRole, (req, res) => {
    const job = jobs.get(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'job not found' });
    const elapsed = ((Date.now() - job.startedAt) / 1000).toFixed(1);
    if (job.status === 'done') {
      jobs.delete(req.params.jobId);
      return res.json({ status: 'done', ranking: job.ranking, elapsed });
    }
    if (job.status === 'error') {
      jobs.delete(req.params.jobId);
      return res.json({ status: 'error', error: job.error });
    }
    res.json({ status: 'running', elapsed, progress: job.progress });
  });
}

module.exports = { registerRankingApi };
