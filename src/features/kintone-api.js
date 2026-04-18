// kintone連携 API（検索・同期）
const { dbEnsureKintoneSchema, dbUpsertKintoneRecords, dbSearchKintoneCompanies, dbGetKintoneLastSync, dbGetKintoneRecord } = require('../db/kintone');
const { syncKintoneApp, APPS } = require('./kintone-sync');

const SYNC_INTERVAL_MS = 30 * 60 * 1000; // 30分
let syncInProgress = false;

async function runSync() {
  if (syncInProgress) return;
  syncInProgress = true;
  try {
    for (const [appId, cfg] of Object.entries(APPS)) {
      console.log(`[kintone] syncing app ${appId}...`);
      const records = await syncKintoneApp(Number(appId), cfg);
      await dbUpsertKintoneRecords(String(appId), records);
      console.log(`[kintone] synced ${records.length} records from app ${appId}`);
    }
  } catch (e) {
    console.error('[kintone] sync error:', e.message);
  } finally {
    syncInProgress = false;
  }
}

function registerKintoneApi({ expressApp, authWithRole, adminOnly }) {
  // 起動時にスキーマ確保 → 初回同期
  dbEnsureKintoneSchema()
    .then(() => runSync())
    .catch(e => console.error('[kintone] init error:', e));

  // 30分ごとに自動同期
  setInterval(runSync, SYNC_INTERVAL_MS);

  // ─────────────────────────────────────────
  // GET /api/kintone/search?q=xxx  企業名部分一致検索
  // ─────────────────────────────────────────
  expressApp.get('/api/kintone/search', authWithRole, async (req, res) => {
    const { q = '' } = req.query;
    if (q.trim().length < 1) return res.json({ results: [] });
    try {
      const results = await dbSearchKintoneCompanies(q.trim(), 10);
      res.json({ results });
    } catch (e) {
      console.error('[kintone] search error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // GET /api/kintone/record/:recordId
  // ─────────────────────────────────────────
  expressApp.get('/api/kintone/record/:recordId', authWithRole, async (req, res) => {
    try {
      const rec = await dbGetKintoneRecord(req.params.recordId);
      if (!rec) return res.status(404).json({ error: 'not_found' });
      res.json({ record: rec });
    } catch (e) {
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // POST /api/kintone/sync  手動同期（adminのみ）
  // ─────────────────────────────────────────
  expressApp.post('/api/kintone/sync', authWithRole, adminOnly, (req, res) => {
    runSync().catch(e => console.error('[kintone] manual sync error:', e));
    res.json({ message: '同期を開始しました' });
  });

  // ─────────────────────────────────────────
  // GET /api/kintone/status  同期状態確認
  // ─────────────────────────────────────────
  expressApp.get('/api/kintone/status', authWithRole, async (req, res) => {
    try {
      const lastSync = await dbGetKintoneLastSync();
      res.json({ lastSync, inProgress: syncInProgress });
    } catch (e) {
      res.status(500).json({ error: 'internal' });
    }
  });
}

module.exports = { registerKintoneApi };
