// daily-report-watcher の「未提出者リスト」投稿を検知して、
// HRMOS データと突き合わせて「出すべき / 要確認 / 休暇確認済み」に自動仕分けし、
// 同スレッドへ返信する機能。
//
// チャンネル: env DAILY_REPORT_CLASSIFIER_CHANNELS（カンマ区切り）
// 検知条件: bot_message + 「未提出」キーワード + 名前リスト含む

const { getToken, getAllUsers, getStampLogsForDate } = require('./ieyasu');

const KINTAI_NOTICE_CHANNEL_ID = process.env.KINTAI_NOTICE_CHANNEL_ID || 'C086FUWG9KR';

// Google Calendar から該当日の休暇イベント検出
// 「有給」「休暇」「休日」「全日」「時間給」「OOO」「不在」などのキーワード + 終日イベント
async function getCalendarVacation(dbQuery, email, dateYmd) {
  if (!email) return null;
  try {
    const tokRes = await dbQuery('SELECT * FROM google_oauth_tokens LIMIT 1');
    const tok = tokRes.rows[0];
    if (!tok?.refresh_token) return null;
    const { google } = require('googleapis');
    const auth = new google.auth.OAuth2(
      process.env.GOOGLE_CLIENT_ID,
      process.env.GOOGLE_CLIENT_SECRET,
      'https://inrevo-task.com/api/google/oauth/callback',
    );
    auth.setCredentials({
      access_token: tok.access_token,
      refresh_token: tok.refresh_token,
      expiry_date: Number(tok.expiry_date),
    });
    const calendar = google.calendar({ version: 'v3', auth });
    const r = await calendar.events.list({
      calendarId: email,
      timeMin: `${dateYmd}T00:00:00+09:00`,
      timeMax: `${dateYmd}T23:59:59+09:00`,
      singleEvents: true,
      maxResults: 50,
    });
    for (const ev of r.data.items || []) {
      const summary = ev.summary || '';
      const isAllDay = !!ev.start?.date;
      // ① eventType=outOfOffice（不在予定）→ 確実に不在
      if (ev.eventType === 'outOfOffice') {
        return { absent: true, reason: summary || '不在設定' };
      }
      // ② 終日イベントで休暇系キーワード → 不在
      if (isAllDay && /休|有給|OOO|不在|国の代わり/.test(summary)) {
        return { absent: true, reason: summary };
      }
      // ③ 時間休/半休（非終日でも対応）
      if (/時間休|半休/.test(summary)) {
        return { absent: true, reason: summary };
      }
    }
    return { absent: false };
  } catch (e) {
    // 404 (calendar not found / not shared) は普通に起こりうる、警告のみ
    console.warn('[dr-classifier] gcal check', email, ':', e.code || e.message);
    return null;
  }
}

// 勤怠連絡チャンネルから欠勤連絡のユーザーIDセットを抽出
// パターン: 「本日は <@U...> 欠勤します。」のような投稿で <@U...> を抽出
async function getAbsenceUsers(client, channelId, dateYmd) {
  if (!channelId) return new Set();
  try {
    const start = Math.floor(new Date(`${dateYmd}T00:00:00+09:00`).getTime() / 1000);
    const end   = Math.floor(new Date(`${dateYmd}T23:59:59+09:00`).getTime() / 1000);
    const r = await client.conversations.history({
      channel: channelId,
      oldest: String(start),
      latest: String(end),
      limit: 200,
    });
    const absent = new Set();
    for (const msg of r.messages || []) {
      const text = msg.text || '';
      if (!/欠勤/.test(text)) continue;
      // 「本日は <@U...> 欠勤」パターン優先
      const m1 = text.match(/本日は\s*<@([A-Z0-9]+)>/);
      if (m1) { absent.add(m1[1]); continue; }
      // フォールバック: 「<@U...> 欠勤」が文中にあれば
      const m2 = text.match(/<@([A-Z0-9]+)>[^<]*欠勤/);
      if (m2) absent.add(m2[1]);
    }
    return absent;
  } catch (e) {
    console.warn('[dr-classifier] absence notice fetch failed:', e.message);
    return new Set();
  }
}

function registerDailyReportClassifier({ app, dbQuery, todayJstYmd }) {
  const channels = new Set(
    (process.env.DAILY_REPORT_CLASSIFIER_CHANNELS || '')
      .split(',').map(s => s.trim()).filter(Boolean)
  );
  // チャンネルID別の type（in/out）判定
  const inChannels  = new Set((process.env.HRMOS_STAMP_IN_CHANNELS  || '').split(',').map(s => s.trim()).filter(Boolean));
  const outChannels = new Set((process.env.HRMOS_STAMP_OUT_CHANNELS || '').split(',').map(s => s.trim()).filter(Boolean));
  if (channels.size === 0) {
    console.log('[dr-classifier] no channels configured, listener inactive');
    return;
  }

  const https = require('https');
  const ieyasuRequest = (path, token) => new Promise(resolve => {
    https.get('https://ieyasu.co' + path, { headers: { Authorization: 'Token ' + token } }, r => {
      let b = '';
      r.on('data', c => b += c);
      r.on('end', () => resolve({ status: r.statusCode, body: b }));
    }).on('error', () => resolve({ status: 0, body: '' }));
  });

  async function getDailyForDate(token, dateYmd) {
    const all = [];
    for (let p = 1; p <= 12; p++) {
      const r = await ieyasuRequest(`/api/inrevo/v1/work_outputs/daily/${dateYmd}?page=${p}`, token);
      if (r.status !== 200) break;
      let arr;
      try { arr = JSON.parse(r.body); } catch { break; }
      if (!Array.isArray(arr) || arr.length === 0) break;
      all.push(...arr);
    }
    return all;
  }

  // 日付ヘルパー: "YYYY-MM-DD" から前日を返す（TZ非依存の純粋日付計算）
  function yesterdayJstYmd(today) {
    const [y, m, d] = today.split('-').map(Number);
    const dt = new Date(Date.UTC(y, m - 1, d));
    dt.setUTCDate(dt.getUTCDate() - 1);
    return dt.toISOString().slice(0, 10);
  }

  // 名前パース: 「姓 名/Romaji」または「姓名 Romaji」形式の行を抽出
  // 例: "外山 雄大/Yudai Toyama" → "外山 雄大"
  //     "萩原隼人 Hayato Hagiwara" → "萩原隼人"
  function parseNames(text) {
    const names = [];
    for (const raw of text.split(/\r?\n+/)) {
      let line = raw.trim();
      line = line.replace(/^[-・•●◯○]\s*/, '');
      if (!line || line.length > 80) continue;
      let jp = null;
      // パターン1: 「漢字+スラッシュ+Latin」（スラッシュ前後のスペース許容）
      let m = line.match(/^([一-鿿ぁ-んァ-ヶー々〆〤\s]{2,20})\/\s*[A-Za-z]/);
      if (m) {
        jp = m[1].trim();
      } else {
        // パターン2: 「漢字 Latin」 (スペース区切り。漢字部分は内部スペース1個まで許容)
        m = line.match(/^([一-鿿ぁ-んァ-ヶー々〆〤]+(?:\s[一-鿿ぁ-んァ-ヶー々〆〤]+)?)\s+[A-Za-z]/);
        if (m) jp = m[1].trim();
      }
      if (!jp || jp.length < 2) continue;
      names.push(jp);
    }
    return names;
  }

  // 名前 → Slack ユーザーID（dashboard_user_directory）解決
  let _dirCache = null;
  let _dirCacheAt = 0;
  async function getUserDir(teamId) {
    const now = Date.now();
    if (_dirCache && now - _dirCacheAt < 5 * 60 * 1000) return _dirCache;
    const res = await dbQuery(
      `SELECT user_id, display_name, real_name, profile_json FROM dashboard_user_directory WHERE team_id=$1`,
      [teamId]
    );
    _dirCache = res.rows;
    _dirCacheAt = now;
    return _dirCache;
  }

  function resolveSlackUser(name, dir) {
    // 完全一致優先、次に display_name の部分一致
    let m = dir.find(u => (u.display_name || '') === name || (u.real_name || '') === name);
    if (m) return m;
    // スラッシュ区切り（"漢字/English"）の場合、漢字側だけで照合
    const jp = name.split('/')[0].trim();
    m = dir.find(u => (u.display_name || '').startsWith(jp) || (u.real_name || '').startsWith(jp));
    if (m) return m;
    // include 検索
    m = dir.find(u => (u.display_name || '').includes(jp) || (u.real_name || '').includes(jp));
    return m || null;
  }

  // セグメント判定
  function isWorkSegment(segment) {
    if (!segment) return false;
    return /^出勤/.test(segment);
  }

  // ── メイン: メッセージ検知 ─────────────────────────────────
  app.event('message', async ({ event, client }) => {
    try {
      if (!channels.has(event.channel)) return;
      // bot 投稿のみ対象
      const isBot = !!(event.bot_id || event.subtype === 'bot_message');
      if (!isBot) return;
      // 自分自身（Pochi）の投稿はスルー
      if (event.username === 'Pochi') return;

      const text = event.text || '';
      // 未提出キーワード
      if (!/未提出|提出.*いただけ|提出.*忘れ/.test(text)) return;
      // 名前リストっぽい（複数行 or 1名でも明示的リスト）
      const names = parseNames(text);
      if (names.length === 0) {
        console.log(`[dr-classifier] no names parsed in ch=${event.channel} ts=${event.ts}`);
        return;
      }

      // 出勤 or 退勤 判定: チャンネルID優先、フォールバックでテキストキーワード
      let isOut = outChannels.has(event.channel);
      let isIn  = inChannels.has(event.channel);
      if (!isOut && !isIn) {
        // チャンネル未マッピング時は親メッセージ or 自テキストから推定
        isOut = /退勤/.test(text);
        isIn  = /出勤/.test(text) && !isOut;
      }
      if (!isIn && !isOut) {
        console.log(`[dr-classifier] type unknown ch=${event.channel}`);
        return;
      }

      const teamId = process.env.SLACK_TEAM_ID || 'T086C06L5V0';
      const today = todayJstYmd();
      // 出勤日報 = 当日 / 退勤日報 = 前日
      const targetDate = isOut ? yesterdayJstYmd(today) : today;
      const reportType = isOut ? '退勤日報' : '出勤日報';

      // HRMOS 当該日のデータ + ユーザー解決
      // - work_outputs/daily: segment_display_title（休暇/祝日判定、ラグ問題なし）
      // - stamp_logs/daily: 実打刻（リアルタイム反映、上書き・二重打刻防止に必要）
      const token = await getToken();
      const [hrmosUsers, dailyAll, stampLogs, dir, absenceUsers] = await Promise.all([
        getAllUsers(token),
        getDailyForDate(token, targetDate),
        getStampLogsForDate(token, targetDate),
        getUserDir(teamId),
        isIn ? getAbsenceUsers(client, KINTAI_NOTICE_CHANNEL_ID, targetDate) : Promise.resolve(new Set()),
      ]);
      const hrmosByEmail = new Map(hrmosUsers.map(u => [(u.email || '').toLowerCase(), u]));
      const dailyByUid = new Map(dailyAll.map(d => [d.user_id, d]));
      // stamp_logs から user_id ごとの出勤(1)/退勤(2) を構築
      const stampsByUid = new Map();
      for (const s of stampLogs || []) {
        const uid = Number(s.user_id);
        if (!stampsByUid.has(uid)) stampsByUid.set(uid, { in: null, out: null });
        const entry = stampsByUid.get(uid);
        const t = String(s.created_at || '').match(/T(\d{2}:\d{2})/);
        const hhmm = t ? t[1] : null;
        if (Number(s.stamp_type) === 1 && !entry.in)  entry.in  = hhmm;
        if (Number(s.stamp_type) === 2 && !entry.out) entry.out = hhmm;
      }

      // Slack メール解決: ディレクトリの profile_json.email を優先（DB保持）、なければ API
      async function getEmail(slackUser) {
        const dirEmail = (slackUser.profile_json?.email || '').toLowerCase();
        if (dirEmail) return dirEmail;
        try {
          const u = await client.users.info({ user: slackUser.user_id });
          return (u.user?.profile?.email || '').toLowerCase();
        } catch { return null; }
      }

      // バケット: submit(出すべき) / check(要確認) / leave(出さなくてOK) / report_only(日報のみ忘れ)
      const buckets = { submit: [], check: [], leave: [], report_only: [] };
      for (const name of names) {
        const slackUser = resolveSlackUser(name, dir);
        if (!slackUser) {
          console.log(`[dr-classifier] resolve fail: ${name}`);
          buckets.check.push({ name, note: 'Slack ユーザー特定不可' });
          continue;
        }
        const email = await getEmail(slackUser);
        if (!email) {
          console.log(`[dr-classifier] email fail: ${name} uid=${slackUser.user_id}`);
          buckets.check.push({ name, note: 'メール未取得' });
          continue;
        }
        const hrUser = hrmosByEmail.get(email);
        if (!hrUser) {
          console.log(`[dr-classifier] hrmos user fail: ${name} email=${email}`);
          buckets.check.push({ name, note: 'HRMOSユーザー未紐付け' });
          continue;
        }
        const daily = dailyByUid.get(hrUser.id);
        // segment（休暇判定）は work_outputs から取得（dailyがない場合は未確定として扱う）
        const segment = daily?.segment_display_title || '';
        // 実打刻は stamp_logs から取得（リアルタイム）
        const stamps = stampsByUid.get(hrUser.id) || { in: null, out: null };
        const hasIn  = !!stamps.in;
        const hasOut = !!stamps.out;

        if (isOut) {
          // 退勤日報（昨日のデータ）
          if (segment && !isWorkSegment(segment)) {
            buckets.leave.push({ name, segment });
          } else if (hasIn && !hasOut) {
            buckets.submit.push({ name, time: stamps.in });
          } else if (hasIn && hasOut) {
            buckets.report_only.push({ name, time: stamps.out });
          } else {
            // 出勤打刻なし → カレンダー確認 → 休暇なら leave、なければ check
            const cal = await getCalendarVacation(dbQuery, email, targetDate);
            if (cal?.absent) buckets.leave.push({ name, segment: `${cal.reason}` });
            else buckets.check.push({ name });
          }
        } else {
          // 出勤日報（当日のデータ）
          if (segment && !isWorkSegment(segment)) {
            buckets.leave.push({ name, segment });
          } else if (hasIn) {
            buckets.submit.push({ name, time: stamps.in });
          } else if (absenceUsers.has(slackUser.user_id)) {
            buckets.leave.push({ name, segment: '欠勤連絡あり' });
          } else {
            // カレンダーで休暇確認
            const cal = await getCalendarVacation(dbQuery, email, targetDate);
            if (cal?.absent) buckets.leave.push({ name, segment: `${cal.reason}` });
            else buckets.check.push({ name });
          }
        }
      }

      // 返信ブロック構築（カテゴリごとに section 分割し Slack の自動省略を回避）
      const totalCategorized = buckets.submit.length + buckets.check.length + buckets.leave.length + buckets.report_only.length;
      const blocks = [
        { type: 'header', text: { type: 'plain_text', text: `🤖 Pochi 自動仕分け（${reportType}） 全${totalCategorized}名` } },
      ];

      function addSection(title, hint, items, formatter) {
        const lines = [`*${title} (${items.length}名)*`];
        if (hint) lines.push(`_${hint}_`);
        if (items.length === 0) lines.push('該当者なし');
        else lines.push(...items.map(formatter));
        blocks.push({ type: 'section', text: { type: 'mrkdwn', text: lines.join('\n') } });
      }

      addSection(
        '📝 出すべき',
        isOut ? '退勤打刻も未実施 → HRMOSで打刻必要' : null,
        buckets.submit,
        u => `• ${u.name}${u.time ? `  _(${u.time} 出勤)_` : ''}`,
      );
      addSection(
        '❓ 要確認',
        null,
        buckets.check,
        u => `• ${u.name}${u.note ? `  _(${u.note})_` : ''}`,
      );
      addSection(
        '🏖️ 出さなくてOK',
        null,
        buckets.leave,
        u => `• ${u.name}${u.segment ? `  _(${u.segment})_` : ''}`,
      );
      // 「日報のみ未提出」セクションは廃止（watcher 通知だけで十分なため）

      const threadTs = event.thread_ts || event.ts;
      await client.chat.postMessage({
        channel: event.channel,
        thread_ts: threadTs,
        text: `Pochi 自動仕分け（${reportType}）`,
        blocks,
        link_names: false,
        unfurl_links: false,
        unfurl_media: false,
      });

      console.log(`[dr-classifier] posted: type=${reportType} submit=${buckets.submit.length} check=${buckets.check.length} leave=${buckets.leave.length} report_only=${buckets.report_only.length}`);
    } catch (e) {
      console.warn('[dr-classifier] error:', e?.data?.error || e.message);
    }
  });

  console.log(`[dr-classifier] active on ${channels.size} channel(s)`);
}

module.exports = { registerDailyReportClassifier };
