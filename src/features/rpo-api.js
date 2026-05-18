// RPO案件管理 API
const { findClientFolder, searchClientFolders, findManagementSheet, parseFolderId } = require('./drive-api');
const {
  dbEnsureRpoSchema,
  dbGetUserRpoAccess,
  dbGetUserDashTeamIds,
  dbListRpoClients,
  dbGetRpoClient,
  dbCreateRpoClient,
  dbUpdateRpoClient,
  dbDeleteRpoClient,
  dbListDashTeamsForRpo,
  dbSetHrDept,
  dbListRpoWorkloadItems,
  dbCreateRpoWorkloadItem,
  dbDeleteRpoWorkloadItem,
  dbListMyRpoTasks,
  dbUpdateRpoWorkloadItem,
  dbListRpoTaskTemplates,
  dbCreateRpoTaskTemplate,
  dbUpdateRpoTaskTemplate,
  dbDeleteRpoTaskTemplate,
  dbApplyTemplates,
  dbListRpoApplicants,
  dbGetRpoApplicant,
  dbCreateRpoApplicant,
  dbUpdateRpoApplicant,
  dbDeleteRpoApplicant,
  dbListApplicantActions,
  dbAddApplicantAction,
  dbListRpoMediaSources,
  dbCreateRpoMediaSource,
  dbUpdateRpoMediaSource,
  dbDeleteRpoMediaSource,
  dbGetRpoMediaSourceByKey,
  dbGenerateMonthlyTasksForClient,
  dbGenerateMonthlyTasksAll,
  dbCountApplicantsByStatus,
  dbSeedMediaMasters,
  dbListMediaMasters,
  dbCreateMediaMaster,
  dbDeleteMediaMaster,
  dbGetRpoSummary,
  dbGetRpoSettings,
  dbSetRpoSetting,
} = require('../db/rpo');
let upload;
try {
  const multer = require('multer');
  upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
} catch {
  // multer未インストール時はCSVインポートエンドポイントが503を返す
  upload = { single: () => (req, res, next) => res.status(503).json({ error: 'csv_import_unavailable' }) };
}

function registerRpoApi({ expressApp, authWithRole, adminOnly }) {
  // 起動時にテーブルを確保
  dbEnsureRpoSchema().catch(e => console.error('[RPO] schema init error:', e));

  // ─────────────────────────────────────────
  // GET /api/rpo/media-masters
  // POST /api/rpo/media-masters
  // DELETE /api/rpo/media-masters/:id
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/media-masters', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbSeedMediaMasters(teamId); // 初回アクセス時にシードを投入
      const masters = await dbListMediaMasters(teamId);
      res.json({ masters });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/rpo/media-masters', authWithRole, async (req, res) => {
    try {
      const { name } = req.body;
      if (!name?.trim()) return res.status(400).json({ error: 'name_required' });
      const master = await dbCreateMediaMaster(req.dashboardUser.teamId, name);
      res.status(201).json({ master });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete('/api/rpo/media-masters/:id', authWithRole, async (req, res) => {
    try {
      await dbDeleteMediaMaster(req.dashboardUser.teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ─────────────────────────────────────────
  // 権限ヘルパー（ミドルウェア + アクセス取得）
  // ─────────────────────────────────────────
  async function withRpoAccess(req, res) {
    const { teamId, userId, role } = req.dashboardUser;
    const access = await dbGetUserRpoAccess(teamId, userId, role);
    if (!access.canAccess) {
      res.status(403).json({ error: 'rpo_forbidden' });
      return null;
    }
    return access;
  }

  // ─────────────────────────────────────────
  // GET /api/rpo/access  自分のアクセス権確認
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/access', authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const access = await dbGetUserRpoAccess(teamId, userId, role);
      // 非フルアクセスの場合、アクセス可能チームの名前を付けて返す
      let myTeams = null;
      if (access.canAccess && !access.fullAccess && access.myTeamIds?.length) {
        const allTeams = await dbListDashTeamsForRpo(teamId);
        myTeams = allTeams.filter(t => access.myTeamIds.includes(t.id));
      }
      res.json({ canAccess: access.canAccess, fullAccess: access.fullAccess || false, myTeams });
    } catch (e) {
      console.error('[RPO] access check error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // GET /api/rpo/teams  チーム一覧（フィルター用）
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/teams', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const teams = await dbListDashTeamsForRpo(teamId);
      res.json({ teams });
    } catch (e) {
      console.error('[RPO] teams error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // PATCH /api/rpo/teams/:id/hr  HR部署フラグ切り替え（adminのみ）
  // ─────────────────────────────────────────
  expressApp.patch('/api/rpo/teams/:id/hr', authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { isHrDept } = req.body;
      await dbSetHrDept(teamId, req.params.id, !!isHrDept);
      res.json({ ok: true });
    } catch (e) {
      console.error('[RPO] toggle hr error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // GET /api/rpo/clients  案件一覧
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/clients', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const filterTeamId = req.query.teamId || null;
      const clients = await dbListRpoClients(teamId, {
        fullAccess:   access.fullAccess,
        myTeamIds:    access.myTeamIds,
        filterTeamId,
      });
      res.json({ clients, access: { canAccess: true, fullAccess: access.fullAccess } });
    } catch (e) {
      console.error('[RPO] list clients error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // GET /api/rpo/clients/:id  案件詳細
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/clients/:id', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const client = await dbGetRpoClient(teamId, req.params.id);
      if (!client) return res.status(404).json({ error: 'not_found' });

      // 非フルアクセスの場合、閲覧できるチームのみ
      if (!access.fullAccess && !access.myTeamIds?.includes(client.dash_team_id)) {
        return res.status(403).json({ error: 'rpo_forbidden' });
      }
      res.json({ client });
    } catch (e) {
      console.error('[RPO] get client error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // POST /api/rpo/clients  案件作成
  // ─────────────────────────────────────────
  expressApp.post('/api/rpo/clients', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;
      const { name, color, plan, dashTeamId, data } = req.body;

      if (!name?.trim()) return res.status(400).json({ error: 'name_required' });

      // Driveフォルダを企業名で自動検索
      const mergedData = data || {};
      if (!mergedData.driveFolder) {
        const settings = await dbGetRpoSettings(teamId);
        const parentId = parseFolderId(settings.driveFolderUrl);
        if (parentId) {
          const folderUrl = await findClientFolder(parentId, name.trim());
          if (folderUrl) mergedData.driveFolder = folderUrl;
        }
      }

      // 管理シートを自動検索してsheetsUrlをセット
      if (mergedData.driveFolder && !mergedData.sheetsUrl) {
        const folderId = parseFolderId(mergedData.driveFolder);
        if (folderId) {
          const sheetUrl = await findManagementSheet(folderId);
          if (sheetUrl) mergedData.sheetsUrl = sheetUrl;
        }
      }

      const client = await dbCreateRpoClient(teamId, {
        name:       name.trim(),
        color:      color || 'Ocean',
        plan:       plan  || 'monthly',
        dashTeamId: dashTeamId || null,
        data:       mergedData,
        createdBy:  userId,
      });
      res.status(201).json({ client });
    } catch (e) {
      console.error('[RPO] create client error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // PUT /api/rpo/clients/:id  案件更新
  // ─────────────────────────────────────────
  expressApp.put('/api/rpo/clients/:id', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const { name, color, plan, status, phase, dashTeamId, data } = req.body;

      const updated = await dbUpdateRpoClient(teamId, req.params.id, { name, color, plan, status, phase, dashTeamId, data });
      if (!updated) return res.status(404).json({ error: 'not_found' });
      res.json({ client: updated });
    } catch (e) {
      console.error('[RPO] update client error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // GET  /api/rpo/my-tasks   自分の全RPOタスク
  // POST /api/rpo/my-tasks   タスク作成
  // PATCH/DELETE /api/rpo/my-tasks/:id
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/my-tasks', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;

      // クライアントドロップダウン: admin でも自分の所属チームのみに絞る
      let clientTeamIds = access.myTeamIds;
      if (!clientTeamIds) {
        clientTeamIds = await dbGetUserDashTeamIds(teamId, userId);
      }
      const clientOpts = clientTeamIds.length > 0
        ? { fullAccess: false, myTeamIds: clientTeamIds }
        : { fullAccess: true };

      const [tasks, clients] = await Promise.all([
        dbListMyRpoTasks(teamId, userId),
        dbListRpoClients(teamId, clientOpts),
      ]);
      res.json({ tasks, clients: clients.filter(c => c.status !== 'archived') });
    } catch (e) {
      console.error('[RPO] my-tasks error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  expressApp.post('/api/rpo/my-tasks', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;
      const { rpoClientId, title, dueDate, taskStatus, notes, statusMemo } = req.body;
      const item = await dbCreateRpoWorkloadItem(teamId, {
        rpoClientId, dashTeamId: null, ownerUserId: userId,
        title: title || '', notes: notes || null,
        dueDate: dueDate || null, statusMemo: statusMemo || null, createdBy: userId,
      });
      if (taskStatus && item) {
        await dbUpdateRpoWorkloadItem(teamId, item.id, { task_status: taskStatus });
        item.task_status = taskStatus;
      }
      res.status(201).json({ item });
    } catch (e) {
      console.error('[RPO] create my-task error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  expressApp.patch('/api/rpo/my-tasks/:id', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const allowed = ['title', 'notes', 'status_memo', 'due_date', 'task_status', 'is_done', 'rpo_client_id'];
      const patch = {};
      for (const k of allowed) if (req.body[k] !== undefined) patch[k] = req.body[k];
      const item = await dbUpdateRpoWorkloadItem(teamId, req.params.id, patch);
      if (!item) return res.status(404).json({ error: 'not_found' });
      res.json({ item });
    } catch (e) {
      console.error('[RPO] patch my-task error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  expressApp.delete('/api/rpo/my-tasks/:id', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      await dbDeleteRpoWorkloadItem(teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) {
      console.error('[RPO] delete my-task error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // GET /api/rpo/clients/:id/workload-items  案件タスク一覧
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/clients/:id/workload-items', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const items = await dbListRpoWorkloadItems(teamId, req.params.id);
      res.json({ items });
    } catch (e) {
      console.error('[RPO] list workload-items error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // POST /api/rpo/clients/:id/workload-items  案件タスク作成
  // ─────────────────────────────────────────
  expressApp.post('/api/rpo/clients/:id/workload-items', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;
      const { dashTeamId, ownerUserId, title, notes, color, dueDate, statusMemo } = req.body;
      const item = await dbCreateRpoWorkloadItem(teamId, {
        rpoClientId: req.params.id,
        dashTeamId:  dashTeamId || null,
        ownerUserId: ownerUserId || null,
        title: title?.trim() || '',
        notes: notes?.trim() || null,
        color: color || null,
        dueDate: dueDate || null,
        statusMemo: statusMemo?.trim() || null,
        createdBy: userId,
      });
      res.status(201).json({ item });
    } catch (e) {
      console.error('[RPO] create workload-item error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // DELETE /api/rpo/clients/:id/workload-items/:itemId  案件タスク削除
  // ─────────────────────────────────────────
  expressApp.delete('/api/rpo/clients/:id/workload-items/:itemId', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      await dbDeleteRpoWorkloadItem(teamId, req.params.itemId);
      res.json({ ok: true });
    } catch (e) {
      console.error('[RPO] delete workload-item error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // DELETE /api/rpo/clients/:id  案件削除（adminのみ）
  // ─────────────────────────────────────────
  expressApp.delete('/api/rpo/clients/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbDeleteRpoClient(teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) {
      console.error('[RPO] delete client error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // タスクテンプレート
  // GET /api/rpo/task-templates
  // POST /api/rpo/task-templates
  // PUT /api/rpo/task-templates/:id
  // DELETE /api/rpo/task-templates/:id
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/task-templates', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const templates = await dbListRpoTaskTemplates(req.dashboardUser.teamId);
      res.json({ templates });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/rpo/task-templates', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { title, notes, sortOrder } = req.body;
      if (!title?.trim()) return res.status(400).json({ error: 'title_required' });
      const t = await dbCreateRpoTaskTemplate(req.dashboardUser.teamId, { title: title.trim(), notes, sortOrder });
      res.status(201).json({ template: t });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/rpo/task-templates/:id', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { title, notes, sortOrder, isActive } = req.body;
      const t = await dbUpdateRpoTaskTemplate(req.dashboardUser.teamId, req.params.id, { title, notes, sortOrder, isActive });
      if (!t) return res.status(404).json({ error: 'not_found' });
      res.json({ template: t });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete('/api/rpo/task-templates/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      await dbDeleteRpoTaskTemplate(req.dashboardUser.teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ─────────────────────────────────────────
  // POST /api/rpo/clients/:id/apply-templates  テンプレートタスクを案件に適用
  // ─────────────────────────────────────────
  expressApp.post('/api/rpo/clients/:id/apply-templates', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;
      const client = await dbGetRpoClient(teamId, req.params.id);
      if (!client) return res.status(404).json({ error: 'not_found' });
      const items = await dbApplyTemplates(teamId, client.id, client.dash_team_id, userId);
      res.json({ items });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ─────────────────────────────────────────
  // 応募者
  // GET  /api/rpo/clients/:id/applicants
  // POST /api/rpo/clients/:id/applicants
  // PUT  /api/rpo/applicants/:apId
  // DELETE /api/rpo/applicants/:apId
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/clients/:id/applicants', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const applicants = await dbListRpoApplicants(teamId, req.params.id, { status: req.query.status || null });
      const counts = await dbCountApplicantsByStatus(teamId, req.params.id);
      res.json({ applicants, counts });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/rpo/clients/:id/applicants', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const { name, status, source, sourceKey, assignedCsUserId, notes, appliedAt, externalId } = req.body;
      if (!name?.trim()) return res.status(400).json({ error: 'name_required' });
      const a = await dbCreateRpoApplicant(teamId, {
        rpoClientId: req.params.id, name: name.trim(), status, source, sourceKey,
        assignedCsUserId, notes, appliedAt, externalId,
      });
      res.status(201).json({ applicant: a });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/rpo/applicants/:apId', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const { name, status, source, sourceKey, assignedCsUserId, notes, appliedAt } = req.body;
      const a = await dbUpdateRpoApplicant(teamId, req.params.apId, { name, status, source, sourceKey, assignedCsUserId, notes, appliedAt });
      if (!a) return res.status(404).json({ error: 'not_found' });
      res.json({ applicant: a });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete('/api/rpo/applicants/:apId', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      await dbDeleteRpoApplicant(req.dashboardUser.teamId, req.params.apId);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ─────────────────────────────────────────
  // 応募者アクション履歴
  // GET  /api/rpo/applicants/:apId/actions
  // POST /api/rpo/applicants/:apId/actions
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/applicants/:apId/actions', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const actions = await dbListApplicantActions(req.dashboardUser.teamId, req.params.apId);
      res.json({ actions });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/rpo/applicants/:apId/actions', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;
      const { type, content } = req.body;
      if (!type) return res.status(400).json({ error: 'type_required' });
      const action = await dbAddApplicantAction(teamId, req.params.apId, { type, content, createdBy: userId });
      res.status(201).json({ action });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ─────────────────────────────────────────
  // 求人媒体連携
  // GET  /api/rpo/media-sources
  // POST /api/rpo/media-sources
  // PUT  /api/rpo/media-sources/:id
  // DELETE /api/rpo/media-sources/:id
  // POST /api/rpo/webhook/:key  (認証不要、外部Webhookエンドポイント)
  // POST /api/rpo/clients/:id/import-csv  CSV一括インポート
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/media-sources', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const sources = await dbListRpoMediaSources(req.dashboardUser.teamId);
      res.json({ sources });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/rpo/media-sources', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { name, config } = req.body;
      if (!name?.trim()) return res.status(400).json({ error: 'name_required' });
      const source = await dbCreateRpoMediaSource(req.dashboardUser.teamId, { name: name.trim(), config });
      res.status(201).json({ source });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/rpo/media-sources/:id', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { name, config, isActive } = req.body;
      const source = await dbUpdateRpoMediaSource(req.dashboardUser.teamId, req.params.id, { name, config, isActive });
      if (!source) return res.status(404).json({ error: 'not_found' });
      res.json({ source });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete('/api/rpo/media-sources/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      await dbDeleteRpoMediaSource(req.dashboardUser.teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // 外部Webhook（認証なし、webhookKeyで識別）
  // シンプルなIPベースレート制限: 1分間に30リクエストまで
  const webhookRateMap = new Map(); // key → { count, resetAt }
  setInterval(() => {
    const now = Date.now();
    for (const [k, v] of webhookRateMap) {
      if (now >= v.resetAt) webhookRateMap.delete(k);
    }
  }, 60_000);

  expressApp.post('/api/rpo/webhook/:key', async (req, res) => {
    const ip = req.ip || req.socket?.remoteAddress || 'unknown';
    const now = Date.now();
    const entry = webhookRateMap.get(ip) || { count: 0, resetAt: now + 60_000 };
    if (now >= entry.resetAt) { entry.count = 0; entry.resetAt = now + 60_000; }
    entry.count += 1;
    webhookRateMap.set(ip, entry);
    if (entry.count > 30) return res.status(429).json({ error: 'rate_limit_exceeded' });
    try {
      const source = await dbGetRpoMediaSourceByKey(req.params.key);
      if (!source) return res.status(404).json({ error: 'invalid_key' });

      const payload = req.body;
      // ペイロードをパースして応募者を登録
      const name = payload.name || payload.applicant_name || payload['氏名'] || '不明';
      const { dbQuery } = require('../db/index');
      // rpo_client_idはソース設定から取得（config.rpoClientIdが必要）
      const rpoClientId = source.config?.rpoClientId;
      if (!rpoClientId) return res.status(400).json({ error: 'source_not_configured' });

      const a = await dbCreateRpoApplicant(source.team_id, {
        rpoClientId,
        name,
        source:    source.name,
        sourceKey: String(payload.id || payload.applicant_id || ''),
        appliedAt: payload.applied_at || payload['応募日'] || new Date().toISOString().slice(0, 10),
        notes:     JSON.stringify(payload),
        externalId: String(payload.id || ''),
      });
      await dbAddApplicantAction(source.team_id, a.id, {
        type: 'webhook_received',
        content: `${source.name} からWebhook受信`,
      });
      res.json({ ok: true, applicantId: a.id });
    } catch (e) {
      console.error('[RPO] webhook error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // CSV一括インポート
  expressApp.post('/api/rpo/clients/:id/import-csv', authWithRole, upload.single('file'), async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;
      const rpoClientId = req.params.id;
      if (!req.file) return res.status(400).json({ error: 'file_required' });

      const csv = req.file.buffer.toString('utf-8');
      const lines = csv.split('\n').filter(l => l.trim());
      if (lines.length < 2) return res.status(400).json({ error: 'empty_csv' });

      // ヘッダー行を解析
      const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
      const nameIdx    = headers.findIndex(h => ['氏名', '名前', 'name', 'applicant_name'].includes(h));
      const statusIdx  = headers.findIndex(h => ['ステータス', 'status'].includes(h));
      const sourceIdx  = headers.findIndex(h => ['媒体', 'source', '求人媒体'].includes(h));
      const dateIdx    = headers.findIndex(h => ['応募日', 'applied_at'].includes(h));
      const notesIdx   = headers.findIndex(h => ['備考', 'notes'].includes(h));

      if (nameIdx === -1) return res.status(400).json({ error: 'no_name_column' });

      const created = [];
      const errors = [];
      for (let i = 1; i < lines.length; i++) {
        const cols = lines[i].split(',').map(c => c.trim().replace(/^"|"$/g, ''));
        const name = cols[nameIdx];
        if (!name) continue;
        try {
          const a = await dbCreateRpoApplicant(teamId, {
            rpoClientId,
            name,
            status:    statusIdx !== -1 ? cols[statusIdx] || '応募' : '応募',
            source:    sourceIdx !== -1 ? cols[sourceIdx] || null : null,
            appliedAt: dateIdx   !== -1 ? cols[dateIdx]   || null : null,
            notes:     notesIdx  !== -1 ? cols[notesIdx]  || null : null,
          });
          created.push(a);
        } catch (err) {
          errors.push({ line: i + 1, name, error: err.message });
        }
      }
      res.json({ imported: created.length, errors });
    } catch (e) {
      console.error('[RPO] csv import error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // 月次タスク自動生成
  // POST /api/rpo/monthly-tasks/generate
  // ─────────────────────────────────────────
  expressApp.post('/api/rpo/monthly-tasks/generate', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;
      const yearMonth = req.body.yearMonth || new Date().toISOString().slice(0, 7); // e.g. "2026-04"
      const results = await dbGenerateMonthlyTasksAll(teamId, yearMonth, userId);
      res.json({ results });
    } catch (e) {
      console.error('[RPO] monthly tasks error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // 個別案件の月次タスク生成
  expressApp.post('/api/rpo/clients/:id/monthly-tasks/generate', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, userId } = req.dashboardUser;
      const yearMonth = req.body.yearMonth || new Date().toISOString().slice(0, 7);
      const result = await dbGenerateMonthlyTasksForClient(teamId, req.params.id, yearMonth, userId);
      res.json(result);
    } catch (e) {
      console.error('[RPO] monthly tasks (single) error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // 工数管理: 月次工数更新
  // PATCH /api/rpo/clients/:id/workload-hours
  // ─────────────────────────────────────────
  expressApp.patch('/api/rpo/clients/:id/workload-hours', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const { yearMonth, hours, otherHours, csAssignee } = req.body;
      if (!yearMonth) return res.status(400).json({ error: 'yearMonth_required' });

      // JSONBの該当フィールドだけ安全に更新
      const { rows } = await require('../db/index').dbQuery(
        `UPDATE rpo_clients
         SET data = jsonb_set(
               jsonb_set(data, '{workloadByMonth,$1}', $2::jsonb, true),
               '{csAssignee}', $3::jsonb, true
             ),
             updated_at = now()
         WHERE id = $4 AND team_id = $5
         RETURNING id`,
        [
          yearMonth,
          JSON.stringify({ hours: Number(hours) || 0, otherHours: Number(otherHours) || 0 }),
          JSON.stringify(csAssignee || null),
          req.params.id,
          teamId,
        ]
      );
      if (!rows.length) return res.status(404).json({ error: 'not_found' });
      res.json({ ok: true });
    } catch (e) {
      console.error('[RPO] workload-hours error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // 全案件サマリー（売上・予算・採用数）
  // GET /api/rpo/summary
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/summary', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId, role } = req.dashboardUser;
      const fullAccess = role === 'admin' || role === 'manager';
      // 非管理者は自分が所属するチームの案件のみ
      let myTeamIds = null;
      if (!fullAccess) {
        myTeamIds = await dbGetUserDashTeamIds(teamId, req.dashboardUser.userId);
      }
      // クエリパラメータでチーム絞り込み
      const filterDashTeamId = req.query.dashTeamId || null;
      const rows = await dbGetRpoSummary(teamId, { fullAccess, myTeamIds, filterDashTeamId });
      res.json({ rows });
    } catch (e) {
      console.error('[RPO] summary error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // RPO設定
  // GET  /api/rpo/settings
  // PUT  /api/rpo/settings
  // ─────────────────────────────────────────
  expressApp.get('/api/rpo/settings', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const settings = await dbGetRpoSettings(req.dashboardUser.teamId);
      res.json({ settings });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // GET /api/rpo/drive/candidates?name=... — Driveフォルダ候補を返す（案件作成時の表記ゆれ対策）
  expressApp.get('/api/rpo/drive/candidates', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const name = (req.query.name || '').trim();
      if (!name || name.length < 1) return res.json({ candidates: [] });

      const settings = await dbGetRpoSettings(teamId);
      const parentId = parseFolderId(settings.driveFolderUrl);
      if (!parentId) return res.json({ candidates: [] });

      const candidates = await searchClientFolders(parentId, name);
      res.json({ candidates });
    } catch (e) {
      console.error('[Drive candidates]', e.message);
      res.json({ candidates: [] });
    }
  });

  expressApp.put('/api/rpo/settings', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const { appsScriptUrl } = req.body;
      if (appsScriptUrl !== undefined) {
        await dbSetRpoSetting(teamId, 'appsScriptUrl', appsScriptUrl || null);
      }
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ─────────────────────────────────────────
  // スプシURL保存（URLのみ、軽量）
  // PATCH /api/rpo/clients/:id/sheets-url
  // ─────────────────────────────────────────
  expressApp.patch('/api/rpo/clients/:id/sheets-url', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const client = await dbGetRpoClient(teamId, req.params.id);
      if (!client) return res.status(404).json({ error: 'not_found' });
      const newData = { ...client.data, sheetsUrl: req.body.sheetsUrl || null };
      await dbUpdateRpoClient(teamId, req.params.id, { data: newData });
      res.json({ ok: true });
    } catch (e) {
      console.error('[RPO] sheets-url error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // DriveフォルダからsheetsUrlを自動検出してセット
  // POST /api/rpo/clients/:id/auto-link-sheets
  // ─────────────────────────────────────────
  expressApp.post('/api/rpo/clients/:id/auto-link-sheets', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;
      const client = await dbGetRpoClient(teamId, req.params.id);
      if (!client) return res.status(404).json({ error: 'not_found' });

      const folderId = parseFolderId(req.body.driveFolder || client.data?.driveFolder);
      if (!folderId) return res.json({ sheetsUrl: null });

      const sheetUrl = await findManagementSheet(folderId);
      if (sheetUrl && sheetUrl !== client.data?.sheetsUrl) {
        await dbUpdateRpoClient(teamId, req.params.id, {
          data: { ...client.data, sheetsUrl: sheetUrl },
        });
      }
      res.json({ sheetsUrl: sheetUrl });
    } catch (e) {
      console.error('[RPO] auto-link-sheets error:', e.message);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // スプシ同期（データはDBに保存せず返すのみ）
  // POST /api/rpo/clients/:id/sheets-sync
  // ─────────────────────────────────────────
  expressApp.post('/api/rpo/clients/:id/sheets-sync', authWithRole, async (req, res) => {
    try {
      const access = await withRpoAccess(req, res);
      if (!access) return;
      const { teamId } = req.dashboardUser;

      const client = await dbGetRpoClient(teamId, req.params.id);
      if (!client) return res.status(404).json({ error: 'not_found' });

      const sheetsUrl = req.body.sheetsUrl || client.data?.sheetsUrl;
      if (!sheetsUrl) return res.status(400).json({ error: 'sheets_url_required' });

      const match = sheetsUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
      if (!match) return res.status(400).json({ error: 'invalid_sheets_url' });
      const sheetId = match[1];

      const settings = await dbGetRpoSettings(teamId);
      const appsScriptUrl = settings.appsScriptUrl;
      if (!appsScriptUrl) return res.status(400).json({ error: 'apps_script_url_not_configured' });

      // シート一覧取得
      const listRes  = await fetch(`${appsScriptUrl}?id=${encodeURIComponent(sheetId)}`);
      const listJson = await listRes.json();
      if (listJson.error) return res.status(502).json({ error: listJson.error });

      const sheetNames = listJson.sheets || [];
      // 「応募者データ」（サフィックスなし）がリストにない場合はフォールバックとして追加
      if (!sheetNames.includes('応募者データ')) sheetNames.push('応募者データ');
      if (!sheetNames.length) return res.status(404).json({ error: 'no_applicant_sheets' });

      // 各シートのデータ取得（並列）
      const results = await Promise.all(
        sheetNames.map(async name => {
          try {
            const r = await fetch(`${appsScriptUrl}?id=${encodeURIComponent(sheetId)}&sheet=${encodeURIComponent(name)}`);
            const j = await r.json();
            return j.error ? null : { name, rows: j.rows || [] };
          } catch { return null; }
        })
      );
      const sheets = results.filter(Boolean);

      // Apps Scriptからバリデーション選択肢を取得（phase=1）
      let phaseOptions = client.data?.phaseOptions || []; // 前回値をデフォルトに
      try {
        const pr = await fetch(`${appsScriptUrl}?id=${encodeURIComponent(sheetId)}&phase=1`);
        const pj = await pr.json();
        const fetched = [...new Set((pj.options || []).filter(v => v && String(v).trim()))];
        if (fetched.length > 0) phaseOptions = fetched;
      } catch { /* Apps Script未対応の場合はスキップ */ }

      // phaseOptionsをDBに保存（永続化）
      if (phaseOptions.length > 0) {
        await dbUpdateRpoClient(teamId, req.params.id, {
          data: { ...client.data, phaseOptions },
        }).catch(() => {});
      }

      res.json({ sheets, syncedAt: new Date().toISOString(), phaseOptions });
    } catch (e) {
      console.error('[RPO] sheets-sync error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });
}

module.exports = { registerRpoApi };
