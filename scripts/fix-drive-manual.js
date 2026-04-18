const { Pool } = require('pg');
const { findClientFolder, parseFolderId } = require('../src/features/drive-api');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const PARENT_FOLDER_URL = 'https://drive.google.com/drive/folders/1jKvYSVwWKHsXyaMIE9sf245yw6zfwGGU';

// dbName: name in DB, driveName: actual folder name in Drive
const CORRECTIONS = [
  { dbName: 'Zilch hair',               driveName: 'zilch hair様' },
  { dbName: '三ッ輪ホールディングス株式会社', driveName: '三ッ輪産業株式会社様' },
  { dbName: '株式会社PORTEHOMME',        driveName: '株式会社PORTE HOMME様' },
];

async function run() {
  const parentId = parseFolderId(PARENT_FOLDER_URL);

  for (const c of CORRECTIONS) {
    // findClientFolder uses two-level traversal, no drive membership needed
    const url = await findClientFolder(parentId, c.driveName);
    if (!url) {
      console.log('Drive folder not found:', c.driveName);
      continue;
    }
    console.log('Drive match:', c.driveName, '->', url);

    const { rows } = await pool.query(`SELECT id, data FROM rpo_clients WHERE name = $1`, [c.dbName]);
    if (!rows.length) { console.log('DB client not found:', c.dbName); continue; }

    const row = rows[0];
    const newData = { ...row.data, driveFolder: url };
    await pool.query('UPDATE rpo_clients SET data = $1 WHERE id = $2', [JSON.stringify(newData), row.id]);
    console.log('UPDATED:', c.dbName);
  }
  await pool.end();
  console.log('Done.');
}

run().catch(e => { console.error(e.message); process.exit(1); });
