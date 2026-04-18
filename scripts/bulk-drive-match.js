const { Pool } = require('pg');
const { findClientFolder, parseFolderId } = require('../src/features/drive-api');

const pool = new Pool(process.env.DATABASE_URL
  ? { connectionString: process.env.DATABASE_URL }
  : {
    user: 'slacktask',
    host: 'localhost',
    database: 'slacktask',
    password: 'slacktask2026',
    port: 5432,
  });

async function run() {
  const PARENT_FOLDER_URL = 'https://drive.google.com/drive/folders/1jKvYSVwWKHsXyaMIE9sf245yw6zfwGGU';
  const parentId = parseFolderId(PARENT_FOLDER_URL);
  console.log('Parent folder ID:', parentId);

  const { rows } = await pool.query(
    `SELECT id, name, data FROM rpo_clients WHERE (data->>'driveFolder') IS NULL OR data->>'driveFolder' = '' ORDER BY name`
  );
  console.log('Clients without driveFolder:', rows.length);

  let matched = 0;
  for (const row of rows) {
    const url = await findClientFolder(parentId, row.name);
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
