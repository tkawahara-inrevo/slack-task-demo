// HRMOS 打刻ラグ実測ポーラー
//
// 30秒毎に HRMOS daily を取得し、新規打刻を検知して反映ラグを記録する。
//
// 目的:
//   1. 「HRMOS daily endpoint の反映ラグ」が本当に発生してるか実測
//   2. 中央値・最悪値を見て対策の方針判断
//   3. 検知した打刻は cache として保存 → bot がより早く検知できる材料に

const { getToken } = require('./ieyasu');
const https = require('https');

const COMPANY = process.env.IEYASU_COMPANY || 'inrevo';

const ieyasuRequest = (path, token) => new Promise(resolve => {
  https.get('https://ieyasu.co' + path, { headers: { Authorization: 'Token ' + token } }, r => {
    let b = '';
    r.on('data', c => b += c);
    r.on('end', () => resolve({ status: r.statusCode, body: b }));
  }).on('error', () => resolve({ status: 0, body: '' }));
});

async function fetchDailyAllPages(token, dateYmd) {
  const all = [];
  for (let p = 1; p <= 20; p++) {
    const r = await ieyasuRequest(`/api/${COMPANY}/v1/work_outputs/daily/${dateYmd}?page=${p}`, token);
    if (r.status !== 200) break;
    let arr;
    try { arr = JSON.parse(r.body); } catch { break; }
    if (!Array.isArray(arr) || arr.length === 0) break;
    all.push(...arr);
  }
  return all;
}

function todayJstYmd() {
  const d = new Date();
  const jst = new Date(d.getTime() + (d.getTimezoneOffset() + 9 * 60) * 60000);
  return jst.toISOString().slice(0, 10);
}

// HH:MM 文字列 を 当日 JST の Date オブジェクトに変換
function hhmmToDateJst(hhmm, dateYmd) {
  if (!hhmm) return null;
  const m = String(hhmm).match(/^(\d{1,2}):(\d{2})/);
  if (!m) return null;
  return new Date(`${dateYmd}T${m[1].padStart(2, '0')}:${m[2]}:00+09:00`);
}

async function ensureSchema(dbQuery) {
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS hrmos_stamp_observations (
      id SERIAL PRIMARY KEY,
      hrmos_user_id INTEGER NOT NULL,
      email TEXT,
      observation_date DATE NOT NULL,
      stamp_type INTEGER NOT NULL CHECK (stamp_type IN (1,2)),
      stamp_time TEXT NOT NULL,          -- "HH:MM" from HRMOS
      stamp_at TIMESTAMPTZ,              -- 当日JSTの HH:MM 時点（推定打刻時刻）
      observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      lag_seconds INTEGER,
      UNIQUE (hrmos_user_id, observation_date, stamp_type, stamp_time)
    );
  `).catch(() => {});
  await dbQuery(
    `CREATE INDEX IF NOT EXISTS idx_hrmos_stamp_obs_date ON hrmos_stamp_observations(observation_date, stamp_type);`,
  ).catch(() => {});
}

async function pollOnce(dbQuery) {
  const date = todayJstYmd();
  let token;
  try {
    token = await getToken();
  } catch (e) {
    console.warn('[hrmos-poller] getToken failed:', e.message);
    return;
  }
  let all;
  try {
    all = await fetchDailyAllPages(token, date);
  } catch (e) {
    console.warn('[hrmos-poller] fetch failed:', e.message);
    return;
  }

  const now = new Date();
  let newCount = 0;
  for (const row of all) {
    const uid = Number(row.user_id);
    if (!uid) continue;
    const email = (row.email || '').toLowerCase() || null;

    for (const type of [1, 2]) {
      const hhmm = type === 1 ? row.stamping_start_at : row.stamping_end_at;
      if (!hhmm) continue;
      const stampAt = hhmmToDateJst(hhmm, date);
      const lagSec = stampAt ? Math.max(0, Math.floor((now.getTime() - stampAt.getTime()) / 1000)) : null;

      // 当該 (uid, date, type, hhmm) が既に記録されていればスキップ（=初回観測のみ記録）
      try {
        const r = await dbQuery(
          `INSERT INTO hrmos_stamp_observations
             (hrmos_user_id, email, observation_date, stamp_type, stamp_time, stamp_at, lag_seconds)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (hrmos_user_id, observation_date, stamp_type, stamp_time) DO NOTHING
           RETURNING id`,
          [uid, email, date, type, hhmm, stampAt, lagSec],
        );
        if (r.rows.length > 0) newCount++;
      } catch (e) {
        // テーブル未作成等は無視
      }
    }
  }
  if (newCount > 0) {
    console.log(`[hrmos-poller] ${date} new observations: ${newCount}`);
  }
}

function registerHrmosStampPoller({ dbQuery }) {
  if (process.env.HRMOS_POLLER_DISABLED === '1') {
    console.log('[hrmos-poller] disabled by env');
    return;
  }
  (async () => {
    await ensureSchema(dbQuery);
    console.log('[hrmos-poller] started (30s interval)');
    // 初回即時 + 30秒毎
    pollOnce(dbQuery).catch(e => console.warn('[hrmos-poller] init err:', e.message));
    setInterval(() => {
      pollOnce(dbQuery).catch(e => console.warn('[hrmos-poller] tick err:', e.message));
    }, 30_000);
  })();
}

module.exports = { registerHrmosStampPoller };
