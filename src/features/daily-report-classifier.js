// daily-report-watcher の「未提出者リスト」投稿を検知して、
// HRMOS データと突き合わせて「出すべき / 要確認 / 休暇確認済み」に自動仕分けし、
// 同スレッドへ返信する機能。
//
// チャンネル: env DAILY_REPORT_CLASSIFIER_CHANNELS（カンマ区切り）
// 検知条件: bot_message + 「未提出」キーワード + 名前リスト含む

const { getToken, getAllUsers } = require('./ieyasu');

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

  // 日付ヘルパー
  function yesterdayJstYmd(today) {
    const d = new Date(`${today}T00:00:00+09:00`);
    d.setDate(d.getDate() - 1);
    return d.toISOString().slice(0, 10);
  }

  // 名前パース: 「姓 名/Romaji」形式の行のみ抽出
  // 例: "外山 雄大/Yudai Toyama" → "外山 雄大"
  function parseNames(text) {
    const names = [];
    for (const raw of text.split(/\r?\n+/)) {
      let line = raw.trim();
      line = line.replace(/^[-・•●◯○]\s*/, '');
      if (!line || line.length > 80) continue;
      // 漢字/かな + スラッシュ + 半角英字 のパターン
      const m = line.match(/^([一-鿿ぁ-んァ-ヶー々〆〤\s]{2,20})\/[A-Za-z]/);
      if (!m) continue;
      const jp = m[1].trim();
      if (jp.length < 2) continue;
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
      const token = await getToken();
      const [hrmosUsers, dailyAll, dir] = await Promise.all([
        getAllUsers(token),
        getDailyForDate(token, targetDate),
        getUserDir(teamId),
      ]);
      const hrmosByEmail = new Map(hrmosUsers.map(u => [(u.email || '').toLowerCase(), u]));
      const dailyByUid = new Map(dailyAll.map(d => [d.user_id, d]));

      // Slack メール解決
      async function getEmail(slackUserId) {
        try {
          const u = await client.users.info({ user: slackUserId });
          return (u.user?.profile?.email || '').toLowerCase();
        } catch { return null; }
      }

      const buckets = { submit: [], check: [], leave: [] };
      for (const name of names) {
        const slackUser = resolveSlackUser(name, dir);
        if (!slackUser) {
          buckets.check.push({ name, note: 'Slack ユーザー特定不可' });
          continue;
        }
        const email = await getEmail(slackUser.user_id);
        if (!email) {
          buckets.check.push({ name });
          continue;
        }
        const hrUser = hrmosByEmail.get(email);
        if (!hrUser) {
          buckets.check.push({ name });
          continue;
        }
        const daily = dailyByUid.get(hrUser.id);
        // HRMOS daily にデータがない（反映途中等）→ 要確認へ
        if (!daily) {
          buckets.check.push({ name, note: 'HRMOSデータ未取得' });
          continue;
        }
        const segment = daily.segment_display_title || '';
        const hasIn = !!daily.stamping_start_at;
        const hasOut = !!daily.stamping_end_at;

        if (!isWorkSegment(segment)) {
          // 休暇/休日
          buckets.leave.push({ name, segment });
        } else if (isOut) {
          // 退勤日報: 昨日出勤打刻あり = 出すべき / なし = 要確認
          if (hasIn) {
            buckets.submit.push({ name, time: daily.stamping_end_at || daily.stamping_start_at });
          } else {
            buckets.check.push({ name });
          }
        } else {
          // 出勤日報: 今日出勤打刻あり = 出すべき / なし = 要確認
          if (hasIn) {
            buckets.submit.push({ name, time: daily.stamping_start_at });
          } else {
            buckets.check.push({ name });
          }
        }
      }

      // 返信ブロック構築
      const blocks = [
        { type: 'header', text: { type: 'plain_text', text: `🤖 Pochi 自動仕分け（${reportType}）` } },
      ];

      const lines = [];
      lines.push(`*📝 出すべき (${buckets.submit.length}名)*`);
      if (buckets.submit.length === 0) lines.push('該当者なし');
      else for (const u of buckets.submit) {
        const timeLabel = isOut ? '退勤' : '出勤';
        lines.push(`• ${u.name}${u.time ? `  _(${u.time} ${timeLabel})_` : ''}`);
      }
      lines.push('');
      lines.push(`*❓ 要確認 (${buckets.check.length}名)*`);
      if (buckets.check.length === 0) lines.push('該当者なし');
      else for (const u of buckets.check) lines.push(`• ${u.name}`);
      lines.push('');
      lines.push(`*🏖️ 休暇確認済み (${buckets.leave.length}名)*`);
      if (buckets.leave.length === 0) lines.push('該当者なし');
      else for (const u of buckets.leave) lines.push(`• ${u.name}${u.segment ? `  _(${u.segment})_` : ''}`);

      blocks.push({
        type: 'section',
        text: { type: 'mrkdwn', text: lines.join('\n') },
      });

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

      console.log(`[dr-classifier] posted: type=${reportType} submit=${buckets.submit.length} check=${buckets.check.length} leave=${buckets.leave.length}`);
    } catch (e) {
      console.warn('[dr-classifier] error:', e?.data?.error || e.message);
    }
  });

  console.log(`[dr-classifier] active on ${channels.size} channel(s)`);
}

module.exports = { registerDailyReportClassifier };
