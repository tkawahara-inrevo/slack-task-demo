// kintone顧客キャッシュ DB操作
const { dbQuery } = require('./index');

async function dbEnsureKintoneSchema() {
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS kintone_cache (
      record_id    TEXT        NOT NULL,
      app_id       TEXT        NOT NULL,
      company_name TEXT        NOT NULL,
      data         JSONB       NOT NULL DEFAULT '{}',
      synced_at    TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (app_id, record_id)
    )
  `);
}

async function dbUpsertKintoneRecords(appId, records) {
  for (const rec of records) {
    await dbQuery(
      `INSERT INTO kintone_cache (record_id, app_id, company_name, data, synced_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (app_id, record_id)
       DO UPDATE SET company_name = EXCLUDED.company_name,
                     data         = EXCLUDED.data,
                     synced_at    = NOW()`,
      [rec.recordId, String(appId), rec.companyName, JSON.stringify(rec.data)]
    );
  }
}

async function dbSearchKintoneCompanies(q, limit = 10) {
  const { rows } = await dbQuery(
    `SELECT record_id, app_id, company_name, data
     FROM kintone_cache
     WHERE company_name ILIKE $1
       AND data->>'受注日' IS NOT NULL
       AND data->>'受注日' != 'null'
       AND data->>'受注日' != ''
     ORDER BY company_name
     LIMIT $2`,
    [`%${q}%`, limit]
  );
  return rows;
}

async function dbGetKintoneLastSync() {
  const { rows } = await dbQuery(`SELECT MAX(synced_at) AS last_sync FROM kintone_cache`);
  return rows[0]?.last_sync || null;
}

async function dbGetKintoneRecord(recordId) {
  const { rows } = await dbQuery(
    `SELECT record_id, app_id, company_name, data FROM kintone_cache WHERE record_id = $1 LIMIT 1`,
    [String(recordId)]
  );
  return rows[0] || null;
}

module.exports = { dbEnsureKintoneSchema, dbUpsertKintoneRecords, dbSearchKintoneCompanies, dbGetKintoneLastSync, dbGetKintoneRecord };
