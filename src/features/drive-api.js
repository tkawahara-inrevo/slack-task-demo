const path = require('path');
const { google } = require('googleapis');

const KEY_PATH = path.join(__dirname, '../../drive-service-account.json');
const SCOPES = ['https://www.googleapis.com/auth/drive.readonly'];

function getDriveClient() {
  const auth = new google.auth.GoogleAuth({ keyFile: KEY_PATH, scopes: SCOPES });
  return google.drive({ version: 'v3', auth });
}

// フォルダID or URL からIDを抽出
function parseFolderId(input) {
  if (!input) return null;
  const m = input.match(/\/folders\/([a-zA-Z0-9_-]+)/);
  if (m) return m[1];
  if (/^[a-zA-Z0-9_-]{10,}$/.test(input.trim())) return input.trim();
  return null;
}

// ファイル種別アイコン
function iconForMime(mimeType) {
  if (mimeType === 'application/vnd.google-apps.folder')       return '📁';
  if (mimeType === 'application/vnd.google-apps.spreadsheet')  return '📊';
  if (mimeType === 'application/vnd.google-apps.document')     return '📄';
  if (mimeType === 'application/vnd.google-apps.presentation') return '📑';
  if (mimeType?.startsWith('image/'))                          return '🖼️';
  if (mimeType?.includes('pdf'))                               return '📕';
  return '📎';
}

const SHARED_DRIVE_OPTS = {
  supportsAllDrives: true,
  includeItemsFromAllDrives: true,
};

async function listFolderChildren(drive, folderId, driveId) {
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

// 共有ドライブのメタ情報とrowフォルダ一覧を取得（内部キャッシュなし、呼び出し元でキャッシュ推奨）
async function getDriveMeta(drive, parentFolderId) {
  const meta = await drive.files.get({
    fileId: parentFolderId,
    fields: 'id,name,driveId',
    supportsAllDrives: true,
  });
  const driveId = meta.data.driveId;
  const rowFolders = await listFolderChildren(drive, parentFolderId, driveId);
  return { driveId, rowFolders };
}

// 全rowフォルダを横断してクエリに一致する会社フォルダを最大 limit 件返す
async function searchClientFolders(parentFolderId, query, limit = 8) {
  try {
    const drive = getDriveClient();
    const { driveId, rowFolders } = await getDriveMeta(drive, parentFolderId);

    const escaped = query.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    const results = [];

    await Promise.all(rowFolders.map(async (rowFolder) => {
      const params = {
        ...SHARED_DRIVE_OPTS,
        q: `'${rowFolder.id}' in parents and mimeType='application/vnd.google-apps.folder' and name contains '${escaped}' and trashed=false`,
        fields: 'files(id,name,webViewLink)',
        pageSize: 20,
      };
      if (driveId) { params.corpora = 'drive'; params.driveId = driveId; }
      const r = await drive.files.list(params);
      for (const f of (r.data.files || [])) {
        results.push({ name: f.name, webViewLink: f.webViewLink });
      }
    }));

    // 完全一致・前方一致・部分一致の順でソート
    results.sort((a, b) => {
      const scoreOf = (name) => {
        if (name === query) return 0;
        if (name.startsWith(query) || query.startsWith(name)) return 1;
        return 2;
      };
      return scoreOf(a.name) - scoreOf(b.name);
    });

    return results.slice(0, limit);
  } catch (e) {
    console.error('[Drive] searchClientFolders error:', e.message);
    return [];
  }
}

// 親フォルダ(共有ドライブ)内から企業名に一致するフォルダを検索して webViewLink を返す
// 会社フォルダは行別サブフォルダ(_あ行など)の2階層目に格納されている
async function findClientFolder(parentFolderId, clientName) {
  try {
    const drive = getDriveClient();
    const { driveId, rowFolders } = await getDriveMeta(drive, parentFolderId);

    // Level 2: search each row-subfolder's children for matching company name
    for (const rowFolder of rowFolders) {
      const escaped = clientName.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
      const params = {
        ...SHARED_DRIVE_OPTS,
        q: `'${rowFolder.id}' in parents and mimeType='application/vnd.google-apps.folder' and name contains '${escaped}' and trashed=false`,
        fields: 'files(id,name,webViewLink)',
        pageSize: 10,
      };
      if (driveId) { params.corpora = 'drive'; params.driveId = driveId; }
      const r = await drive.files.list(params);
      const folders = r.data.files || [];
      const exact   = folders.find(f => f.name === clientName);
      const forward = folders.find(f => f.name.startsWith(clientName) || clientName.startsWith(f.name));
      const match   = exact || forward;
      if (match) return match.webViewLink;
    }
    return null;
  } catch (e) {
    console.error('[Drive] findClientFolder error:', e.message);
    return null;
  }
}

function registerDriveApi({ expressApp, authWithRole }) {
  // GET /api/drive/files?folderId=...
  expressApp.get('/api/drive/files', authWithRole, async (req, res) => {
    try {
      const folderId = parseFolderId(req.query.folderId);
      if (!folderId) return res.status(400).json({ error: 'invalid_folder_id' });

      const drive = getDriveClient();
      const meta = await drive.files.get({
        fileId: folderId,
        fields: 'id,driveId',
        supportsAllDrives: true,
      }).catch(() => ({ data: {} }));
      const driveId = meta.data.driveId;

      const listParams = {
        ...SHARED_DRIVE_OPTS,
        q: `'${folderId}' in parents and trashed = false`,
        fields: 'files(id,name,mimeType,modifiedTime,size,webViewLink)',
        orderBy: 'folder,name',
        pageSize: 200,
      };
      if (driveId) {
        listParams.corpora = 'drive';
        listParams.driveId = driveId;
      }
      const r = await drive.files.list(listParams);

      const files = (r.data.files || []).map(f => ({
        id:           f.id,
        name:         f.name,
        mimeType:     f.mimeType,
        modifiedTime: f.modifiedTime,
        size:         f.size,
        webViewLink:  f.webViewLink,
        icon:         iconForMime(f.mimeType),
        isFolder:     f.mimeType === 'application/vnd.google-apps.folder',
      }));

      res.json({ files, folderId });
    } catch (e) {
      console.error('[Drive]', e.message);
      res.status(500).json({ error: 'drive_error', message: e.message });
    }
  });
}

module.exports = { registerDriveApi, findClientFolder, searchClientFolders, parseFolderId };
