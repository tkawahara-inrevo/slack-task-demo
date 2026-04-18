const { Pool } = require('pg');
const { findManagementSheet, parseFolderId } = require('../src/features/drive-api');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function run() {
  const { rows } = await pool.query(
    `SELECT id, name, data FROM rpo_clients
     WHERE (data->>'driveFolder') IS NOT NULL AND data->>'driveFolder' != ''
       AND ((data->>'sheetsUrl') IS NULL OR data->>'sheetsUrl' = '')
     ORDER BY name`
  );
  console.log('Clients with driveFolder but no sheetsUrl:', rows.length);

  let matched = 0;
  for (const row of rows) {
    const folderId = parseFolderId(row.data.driveFolder);
    if (!folderId) { console.log('skip (no folderId):', row.name); continue; }

    const url = await findManagementSheet(folderId);
    if (url) {
      const newData = { ...row.data, sheetsUrl: url };
      await pool.query('UPDATE rpo_clients SET data = $1 WHERE id = $2', [JSON.stringify(newData), row.id]);
      console.log('LINKED:', row.name, '->', url);
      matched++;
    } else {
      console.log('no sheet found:', row.name);
    }
  }
  console.log('Done:', matched, '/', rows.length);
  await pool.end();
}

run().catch(e => { console.error(e.message); process.exit(1); });
