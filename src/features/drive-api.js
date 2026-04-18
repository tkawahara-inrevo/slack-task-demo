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

// 親フォルダ(共有ドライブ)内から企業名に一致するフォルダを検索して webViewLink を返す
// 会社フォルダは行別サブフォルダに入っているため、ドライブ全体を検索する
async function findClientFolder(parentFolderId, clientName) {
  try {
    const drive = getDriveClient();

    // Get driveId from parent folder metadata (required for Shared Drive corpora)
    const meta = await drive.files.get({
      fileId: parentFolderId,
      fields: 'id,name,driveId',
      supportsAllDrives: true,
    });
    const driveId = meta.data.driveId;

    const escaped = clientName.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    const listParams = {
      ...SHARED_DRIVE_OPTS,
      // Search entire drive (not just direct children) since folders are nested in 行-subfolders
      q: `mimeType='application/vnd.google-apps.folder' and name contains '${escaped}' and trashed=false`,
      fields: 'files(id,name,webViewLink,parents)',
      pageSize: 20,
    };
    if (driveId) {
      listParams.corpora = 'drive';
      listParams.driveId = driveId;
    }

    const r = await drive.files.list(listParams);
    const folders = r.data.files || [];
    const exact   = folders.find(f => f.name === clientName);
    const forward = folders.find(f => f.name.startsWith(clientName) || clientName.startsWith(f.name));
    const match   = exact || forward || folders[0] || null;
    return match ? match.webViewLink : null;
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

module.exports = { registerDriveApi, findClientFolder, parseFolderId };
