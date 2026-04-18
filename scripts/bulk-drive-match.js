const { Pool } = require('pg');
const { google } = require('googleapis');
const path = require('path');

const pool = new Pool(process.env.DATABASE_URL
  ? { connectionString: process.env.DATABASE_URL }
  : {
    user: 'slacktask',
    host: 'localhost',
    database: 'slacktask',
    password: 'slacktask2026',
    port: 5432,
  });

const KEY_PATH = path.join(__dirname, '../drive-service-account.json');
const auth = new google.auth.GoogleAuth({ keyFile: KEY_PATH, scopes: ['https://www.googleapis.com/auth/drive.readonly'] });
const drive = google.drive({ version: 'v3', auth });

const SHARED_DRIVE_OPTS = { supportsAllDrives: true, includeItemsFromAllDrives: true };
const PARENT_FOLDER_ID = '1jKvYSVwWKHsXyaMIE9sf245yw6zfwGGU';

async function listChildren(folderId, driveId) {
  const params = {
    ...SHARED_DRIVE_OPTS,
    q: `'${folderId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
    fields: 'nextPageToken,files(id,name,webViewLink)',
    pageSize: 200,
  };
  if (driveId) { params.corpora = 'drive'; params.driveId = driveId; }

  const files = [];
  let pageToken;
  do {
    if (pageToken) params.pageToken = pageToken;
    const r = await drive.files.list(params);
    files.push(...(r.data.files || []));
    pageToken = r.data.nextPageToken;
  } while (pageToken);
  return files;
}

async function buildFolderMap() {
  console.log('Fetching top-level folders...');
  const meta = await drive.files.get({ fileId: PARENT_FOLDER_ID, fields: 'id,driveId', supportsAllDrives: true });
  const driveId = meta.data.driveId;
  console.log('driveId:', driveId);

  const rowFolders = await listChildren(PARENT_FOLDER_ID, driveId);
  console.log('Row folders:', rowFolders.map(f => f.name));

  const map = new Map(); // name → webViewLink
  await Promise.all(rowFolders.map(async (rowFolder) => {
    const children = await listChildren(rowFolder.id, driveId);
    for (const f of children) {
      map.set(f.name, f.webViewLink);
    }
    console.log(`  ${rowFolder.name}: ${children.length} folders`);
  }));
  return map;
}

function findMatch(map, clientName) {
  if (map.has(clientName)) return map.get(clientName);
  for (const [name, url] of map) {
    if (name.startsWith(clientName) || clientName.startsWith(name)) return url;
  }
  return null;
}

async function run() {
  const folderMap = await buildFolderMap();
  console.log('Total company folders indexed:', folderMap.size);

  const { rows } = await pool.query(
    `SELECT id, name, data FROM rpo_clients WHERE (data->>'driveFolder') IS NULL OR data->>'driveFolder' = '' ORDER BY name`
  );
  console.log('Clients without driveFolder:', rows.length);

  let matched = 0;
  for (const row of rows) {
    const url = findMatch(folderMap, row.name);
    if (url) {
      const newData = { ...row.data, driveFolder: url };
      await pool.query('UPDATE rpo_clients SET data = $1 WHERE id = $2', [JSON.stringify(newData), row.id]);
      console.log('MATCHED:', row.name, '->', url);
      matched++;
    } else {
      console.log('no match:', row.name);
    }
  }
  console.log('Done:', matched, '/', rows.length);
  await pool.end();
}

run().catch(e => { console.error(e.message); process.exit(1); });
