// IEYASU（HRMOS勤怠）連携
// Slack日報投稿 → 自動打刻
const https = require('https');

const IEYASU_TOKEN = process.env.IEYASU_API_TOKEN || '';
// 打刻API: POST /api/v1/attendances
// 種別: clock_in(出勤) / clock_out(退勤)

async function ieyasuPost(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = https.request({
      hostname: 'f.ieyasu.co',
      path,
      method: 'POST',
      headers: {
        'X-Api-Token': IEYASU_TOKEN,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
    }, res => {
      let buf = '';
      res.on('data', c => { buf += c; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(buf) }); }
        catch (e) { resolve({ status: res.statusCode, body: buf }); }
      });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

// Slack ユーザーのメールから IEYASU 社員を特定して打刻する
async function stampAttendance(slackClient, slackUserId, type) {
  if (!IEYASU_TOKEN) {
    console.warn('[IEYASU] IEYASU_API_TOKEN not set, skipping stamp');
    return { ok: false, reason: 'no_token' };
  }

  try {
    // Slackプロフィールからメールを取得
    const profileRes = await slackClient.users.profile.get({ user: slackUserId });
    const email = profileRes.profile?.email;
    if (!email) return { ok: false, reason: 'no_email' };

    // IEYASU で該当社員を検索
    const empRes = await new Promise((resolve, reject) => {
      const req = https.request({
        hostname: 'f.ieyasu.co',
        path: `/api/v1/employees?email=${encodeURIComponent(email)}`,
        method: 'GET',
        headers: { 'X-Api-Token': IEYASU_TOKEN },
      }, res => {
        let buf = '';
        res.on('data', c => { buf += c; });
        res.on('end', () => { try { resolve(JSON.parse(buf)); } catch (e) { resolve({}); } });
      });
      req.on('error', reject);
      req.end();
    });

    const emp = empRes.employees?.[0];
    if (!emp) {
      console.warn(`[IEYASU] employee not found for email: ${email}`);
      return { ok: false, reason: 'employee_not_found', email };
    }

    // 打刻
    const stampRes = await ieyasuPost('/api/v1/attendances', {
      employee_id: emp.id,
      type,          // 'clock_in' or 'clock_out'
      datetime: new Date().toISOString(),
    });

    if (stampRes.status === 200 || stampRes.status === 201) {
      console.log(`[IEYASU] stamped ${type} for ${email}`);
      return { ok: true, type, email };
    } else {
      console.warn(`[IEYASU] stamp failed: ${JSON.stringify(stampRes.body)}`);
      return { ok: false, reason: 'api_error', detail: stampRes.body };
    }
  } catch (e) {
    console.error('[IEYASU] error:', e.message);
    return { ok: false, reason: 'exception', message: e.message };
  }
}

// 今日の打刻状況を取得（ホーム画面表示用）
async function getTodayAttendance(email) {
  if (!IEYASU_TOKEN) return null;
  try {
    const today = new Date().toISOString().split('T')[0];
    const res = await new Promise((resolve, reject) => {
      const req = https.request({
        hostname: 'f.ieyasu.co',
        path: `/api/v1/attendances?date=${today}&email=${encodeURIComponent(email)}`,
        method: 'GET',
        headers: { 'X-Api-Token': IEYASU_TOKEN },
      }, r => {
        let buf = '';
        r.on('data', c => { buf += c; });
        r.on('end', () => { try { resolve(JSON.parse(buf)); } catch (e) { resolve(null); } });
      });
      req.on('error', () => resolve(null));
      req.end();
    });
    return res;
  } catch { return null; }
}

module.exports = { stampAttendance, getTodayAttendance };
