// HRMOS勤怠（ieyasu.co）連携
// Slack日報投稿 → 自動打刻
//
// 確認済みAPIエンドポイント:
//   トークン取得: GET  https://ieyasu.co/api/inrevo/v1/authentication/token
//     Auth: Authorization: Basic {base64(secretKey)}
//   ユーザー一覧: GET  https://ieyasu.co/api/inrevo/v1/users?page=1
//     Auth: Authorization: Token {token}
//   打刻:         POST https://ieyasu.co/api/inrevo/v1/stamp_logs
//     Body: { user_id: number, stamp_type: 1(出勤) | 2(退勤) }

const https = require('https');

// IEYASU_API_TOKEN はHRMOS管理画面で発行したAPIキーをそのままセットする。
// 内部でbase64エンコードせずそのままBasic認証ヘッダーに使う（HRMOSの仕様）。
const SECRET_KEY  = process.env.IEYASU_API_TOKEN || '';
const COMPANY     = 'inrevo';
const BASE_HOST   = 'ieyasu.co';

// ── HTTP ヘルパー ──────────────────────────────────────────────
function ieyasuRequest({ method = 'GET', path, auth, body = null }) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const headers = { 'Authorization': auth };
    if (data) {
      headers['Content-Type'] = 'application/json';
      headers['Content-Length'] = Buffer.byteLength(data);
    }
    const req = https.request({ hostname: BASE_HOST, path, method, headers }, res => {
      let buf = '';
      res.on('data', c => { buf += c; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(buf) }); }
        catch { resolve({ status: res.statusCode, body: buf }); }
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

// ── トークン取得（有効期限内はキャッシュ）─────────────────────
let _cachedToken = null;
let _tokenExpiry = 0;

async function getToken() {
  if (!SECRET_KEY) throw new Error('IEYASU_API_TOKEN not set');
  if (_cachedToken && Date.now() < _tokenExpiry - 60_000) return _cachedToken;

  // SECRET_KEY はすでにBase64済みなのでそのまま使う
  const res = await ieyasuRequest({
    path: `/api/${COMPANY}/v1/authentication/token`,
    auth: `Basic ${SECRET_KEY}`,
  });
  if (res.status !== 200 || !res.body?.token) {
    throw new Error(`HRMOS token error: ${JSON.stringify(res.body)}`);
  }
  _cachedToken = res.body.token;
  _tokenExpiry = new Date(res.body.expired_at).getTime();
  console.log('[HRMOS] token refreshed, expires:', res.body.expired_at);
  return _cachedToken;
}

// ── ユーザー一覧取得（全ページ）────────────────────────────────
let _usersCache = null;
let _usersCacheAt = 0;
const USERS_CACHE_TTL = 30 * 60 * 1000; // 30分

async function getAllUsers(token) {
  if (_usersCache && Date.now() - _usersCacheAt < USERS_CACHE_TTL) return _usersCache;

  const users = [];
  let page = 1;
  while (true) {
    const res = await ieyasuRequest({
      path: `/api/${COMPANY}/v1/users?page=${page}`,
      auth: `Token ${token}`,
    });
    if (res.status !== 200 || !Array.isArray(res.body) || res.body.length === 0) break;
    users.push(...res.body);
    if (res.body.length < 25) break; // 1ページ25件未満なら最終ページ
    page++;
  }

  _usersCache = users;
  _usersCacheAt = Date.now();
  console.log(`[HRMOS] users cached: ${users.length}名`);
  return users;
}

// ── 本日の打刻状況取得（指定ユーザー、work_outputs/daily版） ─────
// 戻り値: { stamping_start_at, stamping_end_at, segment_display_title } | null
// HRMOS の daily エンドポイントは user_id クエリで絞り込めず全社員返るのでページングで探す
// 注意: このエンドポイントはバッチ更新型でラグが大きい（最大30分超）。
// 「打刻されているかの判定」には使わず、segment（休暇判定）等の参考に留めること。
async function getDailyWorkOutput(token, hrmosUserId, dateYmd) {
  for (let page = 1; page <= 20; page++) {
    const res = await ieyasuRequest({
      path: `/api/${COMPANY}/v1/work_outputs/daily/${dateYmd}?page=${page}`,
      auth: `Token ${token}`,
    });
    if (res.status !== 200 || !Array.isArray(res.body) || res.body.length === 0) return null;
    const found = res.body.find(r => Number(r.user_id) === Number(hrmosUserId));
    if (found) return found;
  }
  return null;
}

// ── 本日の生の打刻ログ取得（stamp_logs/daily版、リアルタイム） ──
// 戻り値: [{ user_id, stamp_type, created_at, user_agent, ... }]
// stamp_type: 1=出勤, 2=退勤, 7=休憩開始, 8=休憩終了
// リアルタイム反映されるためこちらを打刻判定に使う。
async function getStampLogsForDate(token, dateYmd) {
  const all = [];
  for (let page = 1; page <= 20; page++) {
    const res = await ieyasuRequest({
      path: `/api/${COMPANY}/v1/stamp_logs/daily/${dateYmd}?limit=100&page=${page}`,
      auth: `Token ${token}`,
    });
    if (res.status !== 200 || !Array.isArray(res.body) || res.body.length === 0) break;
    all.push(...res.body);
    if (res.body.length < 100) break;
  }
  return all;
}

// 指定ユーザーの本日の指定種別打刻を探す
// 戻り値: { stampedAt: "HH:MM:SS", rawCreatedAt: "ISO" } | null
async function findStampForUserToday(token, hrmosUserId, type, dateYmd) {
  const logs = await getStampLogsForDate(token, dateYmd);
  const found = logs.find(x => Number(x.user_id) === Number(hrmosUserId) && Number(x.stamp_type) === Number(type));
  if (!found) return null;
  const t = String(found.created_at || '').match(/T(\d{2}:\d{2}:\d{2})/);
  return { stampedAt: t ? t[1] : null, rawCreatedAt: found.created_at };
}

// ── メインの打刻関数 ───────────────────────────────────────────
// type: 1=出勤 / 2=退勤
// 戻り値: { ok, skipped?, alreadyAt?, type, typeName, email, userId, reason? }
async function stampAttendance(slackClient, slackUserId, type) {
  if (!SECRET_KEY) {
    console.warn('[HRMOS] IEYASU_API_TOKEN not set');
    return { ok: false, reason: 'no_token' };
  }

  try {
    // 1. Slackメール取得
    const userRes = await slackClient.users.info({ user: slackUserId });
    const email = (userRes.user?.profile?.email || '').toLowerCase();
    if (!email) return { ok: false, reason: 'no_email' };

    // 2. トークン取得
    const token = await getToken();

    // 3. HRMOSユーザーをメールで検索
    const users = await getAllUsers(token);
    const hrUser = users.find(u => (u.email || '').toLowerCase() === email);
    if (!hrUser) {
      console.warn(`[HRMOS] user not found: ${email}`);
      return { ok: false, reason: 'user_not_found', email };
    }

    const typeName = type === 1 ? '出勤' : '退勤';

    // 4. 二重打刻チェック: stamp_logs/daily（リアルタイム）で本日の打刻状況を確認
    //   - 既に同種別の打刻があれば常にスキップ（手動打刻を上書きしない）
    //   - stamp_logs API は生ログ取得（リアルタイム反映）
    //     → 旧 work_outputs/daily の集計バッチ更新ラグ（最大30分超）問題を解消
    const now = new Date();
    const jst = new Date(now.getTime() + (now.getTimezoneOffset() + 9 * 60) * 60000);
    const todayYmd = jst.toISOString().slice(0, 10);
    try {
      const existing = await findStampForUserToday(token, hrUser.id, type, todayYmd);
      if (existing) {
        console.log(`[HRMOS] ${typeName}打刻 既存 ${existing.stampedAt} → skip（stamp_logs検知）`);
        return {
          ok: true, skipped: true,
          alreadyAt: existing.stampedAt,
          type, typeName, email, userId: hrUser.id,
        };
      }
    } catch (e) {
      // チェック失敗時はそのまま打刻に進む（致命的ではない）
      console.warn('[HRMOS] stamp_logs check error (continuing):', e.message);
    }

    // 5. 打刻
    const stampRes = await ieyasuRequest({
      method: 'POST',
      path: `/api/${COMPANY}/v1/stamp_logs`,
      auth: `Token ${token}`,
      body: { user_id: hrUser.id, stamp_type: type },
    });

    if (stampRes.status === 200 || stampRes.status === 201) {
      console.log(`[HRMOS] ${typeName}打刻 OK: ${email} (user_id=${hrUser.id})`);
      return { ok: true, type, typeName, email, userId: hrUser.id };
    } else {
      console.warn(`[HRMOS] stamp failed:`, stampRes.body);
      return { ok: false, reason: 'api_error', detail: stampRes.body, userId: hrUser.id };
    }
  } catch (e) {
    console.error('[HRMOS] stampAttendance error:', e.message);
    return { ok: false, reason: 'exception', message: e.message };
  }
}

// ── ユーザーキャッシュ無効化（再取得強制）─────────────────────
function invalidateUsersCache() {
  _usersCache = null;
}

// ── 月次の日次勤怠データ取得（全社員）──────────────────────────
// month: 'YYYY-MM' 形式
// 戻り値: WorkOutput[] (全社員 × 全日)
// キャッシュ: 同月のリクエストは10分間メモリキャッシュ
const _monthlyCache = new Map(); // key=month -> { at, data }
const MONTHLY_TTL = 10 * 60 * 1000;

async function getMonthlyWorkOutputs(month, { forceRefresh = false } = {}) {
  if (!SECRET_KEY) throw new Error('IEYASU_API_TOKEN not set');
  if (!forceRefresh) {
    const c = _monthlyCache.get(month);
    if (c && Date.now() - c.at < MONTHLY_TTL) return c.data;
  }
  const token = await getToken();

  // limit は API側でキャップされる可能性があるため、1ページ目の実件数を基準にする
  const fetchPage = (page) => ieyasuRequest({
    path: `/api/${COMPANY}/v1/work_outputs/monthly/${month}?page=${page}&limit=500`,
    auth: `Token ${token}`,
  });

  const first = await fetchPage(1);
  if (first.status !== 200) throw new Error(`HRMOS work_outputs error: ${first.status} ${JSON.stringify(first.body).slice(0,200)}`);
  if (!Array.isArray(first.body)) return [];
  const pageSize = first.body.length;
  const all = [...first.body];

  // 1ページ目が空 or 期待より少なければ終わり
  if (pageSize > 0) {
    // 残ページを並列取得。「空ページが返るまで」進む（最大50ページ）
    const MAX_PAGES = 50;
    let page = 2;
    while (page <= MAX_PAGES) {
      const batchPages = Array.from({ length: Math.min(5, MAX_PAGES - page + 1) }, (_, i) => page + i);
      const results = await Promise.all(batchPages.map(p => fetchPage(p).catch(e => ({ status: 0, body: null }))));
      let stop = false;
      for (const r of results) {
        if (r.status !== 200 || !Array.isArray(r.body) || r.body.length === 0) { stop = true; break; }
        all.push(...r.body);
        if (r.body.length < pageSize) { stop = true; break; }
      }
      if (stop) break;
      page += batchPages.length;
    }
  }

  console.log(`[HRMOS] monthly ${month}: ${all.length} records (pageSize=${pageSize})`);
  _monthlyCache.set(month, { at: Date.now(), data: all });
  return all;
}

function invalidateMonthlyCache(month) {
  if (month) _monthlyCache.delete(month);
  else _monthlyCache.clear();
}

// 本日打刻済みユーザーをHRMOSから取得（全件・ページング）
// 戻り値: [{user_id, full_name, email, stamping_start_at, stamping_end_at}, ...]
async function getStampedUsersForDay(dateYmd) {
  const token = await getToken();
  const users = await getAllUsers(token);
  const emailMap = new Map(users.map(u => [u.id, (u.email || '').toLowerCase()]));
  const stamped = [];
  for (let p = 1; p <= 20; p++) {
    const res = await ieyasuRequest({
      path: `/api/${COMPANY}/v1/work_outputs/daily/${dateYmd}?page=${p}`,
      auth: `Token ${token}`,
    });
    if (res.status !== 200 || !Array.isArray(res.body) || res.body.length === 0) break;
    for (const r of res.body) {
      if (r.stamping_start_at) {
        stamped.push({
          user_id: r.user_id,
          full_name: r.full_name,
          email: emailMap.get(r.user_id) || null,
          stamping_start_at: r.stamping_start_at,
          stamping_end_at: r.stamping_end_at,
        });
      }
    }
  }
  return stamped;
}

// HRMOS daily endpoint からユーザー単体の打刻状況を取得（slack user_id → メール → HRMOSユーザー解決込み）
// 戻り値: { todayIn, todayOut, yesterdayIn, yesterdayOut, todayInMin }（HH:MM の分単位）
async function getUserDailyContext(slackClient, slackUserId) {
  const userRes = await slackClient.users.info({ user: slackUserId });
  const email = (userRes.user?.profile?.email || '').toLowerCase();
  if (!email) return null;
  const token = await getToken();
  const users = await getAllUsers(token);
  const hrUser = users.find(u => (u.email || '').toLowerCase() === email);
  if (!hrUser) return null;

  const now = new Date();
  const jst = new Date(now.getTime() + (now.getTimezoneOffset() + 9 * 60) * 60000);
  const todayYmd = jst.toISOString().slice(0, 10);
  const ydJst = new Date(jst.getTime() - 24 * 3600 * 1000);
  const yesterdayYmd = ydJst.toISOString().slice(0, 10);

  const [todayD, ydD] = await Promise.all([
    getDailyWorkOutput(token, hrUser.id, todayYmd).catch(() => null),
    getDailyWorkOutput(token, hrUser.id, yesterdayYmd).catch(() => null),
  ]);

  const parseHHMM = (s) => {
    if (!s) return null;
    const m = String(s).match(/^(\d{1,2}):(\d{2})/);
    return m ? Number(m[1]) * 60 + Number(m[2]) : null;
  };

  return {
    todayIn: todayD?.stamping_start_at || null,
    todayOut: todayD?.stamping_end_at || null,
    yesterdayIn: ydD?.stamping_start_at || null,
    yesterdayOut: ydD?.stamping_end_at || null,
    todayInMin: parseHHMM(todayD?.stamping_start_at),
    yesterdayOutMin: parseHHMM(ydD?.stamping_end_at),
    nowJstMin: jst.getUTCHours() * 60 + jst.getUTCMinutes(),
    hrmosUserId: hrUser.id,
    email,
  };
}

module.exports = { stampAttendance, getToken, getAllUsers, invalidateUsersCache, getMonthlyWorkOutputs, invalidateMonthlyCache, getStampedUsersForDay, getUserDailyContext, getStampLogsForDate, findStampForUserToday };
