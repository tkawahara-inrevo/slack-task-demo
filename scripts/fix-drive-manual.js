const { Pool } = require('pg');
const { google } = require('googleapis');
const path = require('path');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const KEY_PATH = path.join(__dirname, '../drive-service-account.json');
const auth = new google.auth.GoogleAuth({ keyFile: KEY_PATH, scopes: ['https://www.googleapis.com/auth/drive.readonly'] });
const drive = google.drive({ version: 'v3', auth });

const CORRECTIONS = [
  { dbName: 'Zilch hair',               driveName: 'zilch hair様' },
  { dbName: '三ッ輪ホールディングス株式会社', driveName: '三ッ輪産業株式会社様' },
  { dbName: '株式会社PORTEHOMME',        driveName: '株式会社PORTE HOMME様' },
];

async function findByName(name, driveId) {
  const escaped = name.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  const r = await drive.files.list({
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    corpora: 'drive',
    driveId,
    q: `mimeType='application/vnd.google-apps.folder' and name = '${escaped}' and trashed=false`,
    fields: 'files(id,name,webViewLink)',
    pageSize: 5,
  });
  return r.data.files || [];
}

async function run() {
  const driveId = '0AKMkvQUfWu5dUk9PVA';

  for (const c of CORRECTIONS) {
    const matches = await findByName(c.driveName, driveId);
    if (!matches.length) {
      console.log('Drive folder not found:', c.driveName);
      continue;
    }
    const url = matches[0].webViewLink;
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
