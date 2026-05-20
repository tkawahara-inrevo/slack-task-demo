// Slackランキング - オンデマンド集計
const CHAT_CHANNEL_ID       = process.env.RANKING_CHAT_CHANNEL_ID || '';
const REPORT_IN_CHANNEL_ID  = process.env.RANKING_REPORT_IN_CHANNEL_ID || '';
const REPORT_OUT_CHANNEL_ID = process.env.RANKING_REPORT_OUT_CHANNEL_ID || '';
const PARENT_LOOKBACK_DAYS  = Number(process.env.RANKING_PARENT_LOOKBACK_DAYS || 2);

// daily-report-watcher の通知投稿 → スレッド返信カウント対象外
const EXCLUDE_KEYWORDS = ['未提出者をお知らせ', '未提出者はいません', '未提出者はいません'];

const CHAT_CH       = { id: CHAT_CHANNEL_ID,       name: 'all-雑談',    category: 'chat' };
const REPORT_IN_CH  = { id: REPORT_IN_CHANNEL_ID,  name: 'all-出勤日報', category: 'report_in_reply' };
const REPORT_OUT_CH = { id: REPORT_OUT_CHANNEL_ID, name: 'all-退勤日報', category: 'report_out_reply' };

function needsChannel(category, ch) {
  if (ch.category === 'chat')            return ['all', 'chat'].includes(category);
  if (ch.category === 'report_in_reply') return ['all', 'report_in_reply', 'report_reply'].includes(category);
  if (ch.category === 'report_out_reply')return ['all', 'report_out_reply', 'report_reply'].includes(category);
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
// 未提出検知botの通知 → このスレッドへの返信はカウントしない
function isExcluded(msg) {
  return EXCLUDE_KEYWORDS.some(kw => (msg.text || '').includes(kw));
}

// ── レート制限対応 API呼び出し ──────────────────────────────
// Tier3: 50+回/分 → 1200ms間隔を目安に、429時はリトライ
let _lastReplyCall = 0;
const REPLY_INTERVAL = 1200;

async function fetchReplies(client, channelId, ts) {
  const wait = REPLY_INTERVAL - (Date.now() - _lastReplyCall);
  if (wait > 0) await sleep(wait);
  _lastReplyCall = Date.now();

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      let cursor, all = [];
      do {
        const res = await client.conversations.replies({ channel: channelId, ts, limit: 200, cursor });
        all.push(...(res.messages || []));
        cursor = res.response_metadata?.next_cursor;
      } while (cursor);
      return all;
    } catch (e) {
      if (e?.data?.error === 'ratelimited' || e?.code === 'slack_webapi_platform_error') {
        const retryAfter = (e?.data?.retry_after || 10) * 1000;
        console.warn(`[ranking] rate limited, waiting ${retryAfter}ms`);
        await sleep(retryAfter);
      } else {
        console.error('[ranking] replies error:', e?.data?.error || e?.message);
        return [];
      }
    }
  }
  return [];
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

async function fetchUserNames(client) {
  let cursor, members = [];
  do {
    const res = await client.users.list({ limit: 200, cursor });
    members.push(...(res.members || []));
    cursor = res.response_metadata?.next_cursor;
  } while (cursor);
  const map = {};
  for (const u of members) map[u.id] = u.profile?.display_name || u.real_name || u.name || u.id;
  return map;
}

// ── チャンネル別集計 ──────────────────────────────────────
async function collectChannel(client, channelConfig, from, to, counter, onProgress) {
  if (!channelConfig.id) return;

  // 出退勤は親メッセージを少し遡って取得（月初めのスレッドを拾うため）
  const oldest = toSlackTs(channelConfig.category === 'chat' ? from : (() => {
    const d = new Date(from); d.setDate(d.getDate() - PARENT_LOOKBACK_DAYS); return d;
  })());

  const messages = await fetchHistory(client, channelConfig.id, oldest, toSlackTs(to));
  onProgress?.({ channel: channelConfig.name, phase: 'fetched', total: messages.length });

  const key = channelConfig.category === 'report_in_reply' ? 'report_in'
            : channelConfig.category === 'report_out_reply' ? 'report_out'
            : 'chat';

  const inc = (userId) => {
    if (!counter[userId]) counter[userId] = { chat: 0, report_in: 0, report_out: 0 };
    counter[userId][key]++;
  };

  if (channelConfig.category === 'chat') {
    // 雑談：トップレベル投稿 + スレッド返信を各1件ずつカウント
    for (const msg of messages) {
      if (isHuman(msg) && inRange(msg.ts, from, to)) inc(msg.user);
    }
    // スレッド返信（人間の投稿のスレッドのみ）
    const threadsToFetch = messages.filter(m => isHuman(m) && m.reply_count > 0);
    let done = 0;
    for (const msg of threadsToFetch) {
      const replies = await fetchReplies(client, channelConfig.id, msg.ts);
      for (const r of replies.slice(1)) {
        if (isHuman(r) && inRange(r.ts, from, to)) inc(r.user);
      }
      done++;
      onProgress?.({ channel: channelConfig.name, phase: 'progress', done, total: threadsToFetch.length });
    }

  } else {
    // 出退勤：トップレベル投稿はカウントしない
    // スレッド返信のみカウント（未提出検知botのスレッドは除外）
    const threadsToFetch = messages.filter(m => m.reply_count > 0 && !isExcluded(m));
    let done = 0;
    for (const msg of threadsToFetch) {
      const replies = await fetchReplies(client, channelConfig.id, msg.ts);
      for (const r of replies.slice(1)) {
        if (isHuman(r) && inRange(r.ts, from, to)) inc(r.user);
      }
      done++;
      onProgress?.({ channel: channelConfig.name, phase: 'progress', done, total: threadsToFetch.length });
    }
  }

  onProgress?.({ channel: channelConfig.name, phase: 'done', total: messages.length });
  console.log(`[ranking] ${channelConfig.name}: done`);
}

async function collectAndAggregate(client, from, to, category, limit, onProgress) {
  const nameMap = await fetchUserNames(client);
  onProgress?.({ step: 'users_loaded' });

  const counter = {};
  const channels = [CHAT_CH, REPORT_IN_CH, REPORT_OUT_CH].filter(ch => needsChannel(category, ch) && ch.id);

  // チャンネルは直列処理（APIレート制限の超過防止）
  for (const ch of channels) {
    await collectChannel(client, ch, from, to, counter, onProgress);
  }

  const sortFn = {
    'all':              (a, b) => b.total - a.total,
    'chat':             (a, b) => b.chat_count - a.chat_count,
    'report_in_reply':  (a, b) => b.report_in_count - a.report_in_count,
    'report_out_reply': (a, b) => b.report_out_count - a.report_out_count,
    'report_reply':     (a, b) => (b.report_in_count + b.report_out_count) - (a.report_in_count + a.report_out_count),
  }[category] || ((a, b) => b.total - a.total);

  let ranking = Object.entries(counter).map(([userId, c]) => ({
    user_id: userId,
    user_name: nameMap[userId] || userId,
    chat_count:        c.chat,
    report_in_count:   c.report_in,
    report_out_count:  c.report_out,
    total: c.chat + c.report_in + c.report_out,
  })).sort(sortFn);

  if (limit > 0) ranking = ranking.slice(0, limit);
  return ranking.map((r, i) => ({ ...r, rank: i + 1 }));
}

// ─── ジョブ管理 ────────────────────────────────────────────
const jobs = new Map();

function registerRankingApi({ expressApp, authWithRole, slackClient }) {
  // user tokenがあれば使用（conversations.repliesにuser tokenが必要な場合）
  const userToken = process.env.RANKING_SLACK_USER_TOKEN;
  const client = userToken
    ? new (require('@slack/web-api').WebClient)(userToken)
    : slackClient;

  expressApp.post('/api/dashboard/ranking/start', authWithRole, async (req, res) => {
    const { from, to, category = 'all', limit = '20' } = req.body || {};
    if (!from || !to) return res.status(400).json({ error: 'from/to required' });
    if (!CHAT_CHANNEL_ID && !REPORT_IN_CHANNEL_ID && !REPORT_OUT_CHANNEL_ID)
      return res.status(400).json({ error: 'channel env vars not set' });

    const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const channels = [CHAT_CH, REPORT_IN_CH, REPORT_OUT_CH]
      .filter(ch => needsChannel(category, ch) && ch.id).map(ch => ch.name);

    jobs.set(jobId, {
      status: 'running', ranking: null, error: null,
      startedAt: Date.now(),
      progress: { channels, done: [], current: null, threadsDone: 0, threadsTotal: 0 },
    });

    (async () => {
      try {
        const fromDate = new Date(`${from}T00:00:00+09:00`);
        const toDate   = new Date(`${to}T23:59:59+09:00`);
        const ranking  = await collectAndAggregate(
          client, fromDate, toDate, category, Number(limit),
          ({ channel, phase, done, total }) => {
            const job = jobs.get(jobId);
            if (!job) return;
            if (phase === 'fetched') { job.progress.current = channel; }
            if (phase === 'progress') { job.progress.threadsDone = done; job.progress.threadsTotal = total; }
            if (phase === 'done') { job.progress.done.push(channel); job.progress.current = null; }
          }
        );
        const job = jobs.get(jobId);
        if (job) { job.status = 'done'; job.ranking = ranking; }
      } catch (e) {
        console.error(`[ranking] job ${jobId} error:`, e.message);
        const job = jobs.get(jobId);
        if (job) { job.status = 'error'; job.error = e.message; }
      }
    })();

    res.json({ jobId, channels });
  });

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
