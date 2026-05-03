const BASE = '/api/dashboard';
const CRM = '/api/crm';
const RPO = '/api/rpo';
const KINTONE = '/api/kintone';

async function apiFetch(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, {
    credentials: 'include',
    ...opts,
  });
  if (res.status === 401) {
    window.location.href = '/dashboard/unauthorized';
    throw new Error('Unauthorized');
  }
  if (res.status === 403) throw new Error('Forbidden');
  if (!res.ok) {
    let msg = `API error: ${res.status}`;
    try { const body = await res.json(); if (body.message) msg = body.message; } catch {}
    throw new Error(msg);
  }
  return res.json();
}

function jsonPost(path, body) {
  return apiFetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

function jsonPut(path, body) {
  return apiFetch(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

function apiDelete(path) {
  return apiFetch(path, { method: 'DELETE' });
}

async function kintoneFetch(path, opts = {}) {
  const res = await fetch(`${KINTONE}${path}`, { credentials: 'include', ...opts });
  if (res.status === 401) { window.location.href = '/dashboard/unauthorized'; throw new Error('Unauthorized'); }
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

async function driveFetch(path, opts = {}) {
  const res = await fetch(`/api/drive${path}`, { credentials: 'include', ...opts });
  if (res.status === 401) { window.location.href = '/dashboard/unauthorized'; throw new Error('Unauthorized'); }
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

async function rpoFetch(path, opts = {}) {
  const res = await fetch(`${RPO}${path}`, { credentials: 'include', ...opts });
  if (res.status === 401) { window.location.href = '/dashboard/unauthorized'; throw new Error('Unauthorized'); }
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
function rpoPost(path, body) {
  return rpoFetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
}

async function crmFetch(path, opts = {}) {
  const res = await fetch(`${CRM}${path}`, { credentials: 'include', ...opts });
  if (res.status === 401) { window.location.href = '/dashboard/unauthorized'; throw new Error('Unauthorized'); }
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
function crmPost(path, body) {
  return crmFetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
}
function crmPut(path, body) {
  return crmFetch(path, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
}
function crmDelete(path) {
  return crmFetch(path, { method: 'DELETE' });
}

export const api = {
  // General
  me: () => apiFetch('/me'),
  summary: () => apiFetch('/summary'),
  tasks: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return apiFetch(`/tasks?${qs}`);
  },
  members: () => apiFetch('/members'),
  personalFilters: () => apiFetch('/personal-filters'),
  usergroups: () => apiFetch('/usergroups'),
  overdue: () => apiFetch('/overdue'),
  myTeams: () => apiFetch('/my-teams'),
  projects: () => apiFetch('/projects'),
  projectTasks: (id) => apiFetch(`/projects/${id}/tasks`),
  workloadTeams: () => apiFetch('/workload/teams'),
  workloadUsers: (teamId) => apiFetch(`/workload/users?teamId=${encodeURIComponent(teamId)}`),
  workloadData: (teamId, month) => apiFetch(`/workload?teamId=${encodeURIComponent(teamId)}&month=${encodeURIComponent(month)}`),
  createWorkloadItem: (body) => jsonPost('/workload/items', body),
  updateWorkloadItem: (id, body) => apiFetch(`/workload/items/${id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }),
  deleteWorkloadItem: (id) => apiDelete(`/workload/items/${id}`),
  setWorkloadCells: (body) => jsonPut('/workload/cells', body),
  copyPreviousWorkloadMonth: (dashTeamId, monthKey) => jsonPost('/workload/copy-prev', { dashTeamId, monthKey }),
  workloadCategories: (dashTeamId) => apiFetch(`/workload/categories?dashTeamId=${encodeURIComponent(dashTeamId)}`),
  createWorkloadCategory: (body) => jsonPost('/workload/categories', body),
  updateWorkloadCategory: (id, body) => apiFetch(`/workload/categories/${id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }),
  deleteWorkloadCategory: (id) => apiDelete(`/workload/categories/${id}`),

  orgChart: () => apiFetch('/org-chart'),

  // Admin: roles
  adminRoles: () => apiFetch('/admin/roles'),
  adminSetRole: (userId, role) => jsonPost('/admin/roles', { userId, role }),
  adminVisibility: (userId) => apiFetch(`/admin/visibility/${encodeURIComponent(userId)}`),
  adminSetVisibility: (userId, visibleDashTeamIds, visibleUserIds) => jsonPut(`/admin/visibility/${encodeURIComponent(userId)}`, {
    visibleDashTeamIds,
    visibleUserIds,
  }),

  // Admin: teams
  adminTeams: () => apiFetch('/admin/teams'),
  adminCreateTeam: (name, parentId = null, extra = {}) => jsonPost('/admin/teams', { name, parentId, ...extra }),
  adminUpdateTeam: (id, name) => jsonPut(`/admin/teams/${id}`, { name }),
  adminUpdateTeamParent: (id, parentId) => jsonPut(`/admin/teams/${id}`, { parentId }),
  adminDeleteTeam: (id) => apiDelete(`/admin/teams/${id}`),

  // Admin: team members
  adminTeamMembers: (id) => apiFetch(`/admin/teams/${id}/members`),
  adminAddTeamMember: (id, userId) => jsonPost(`/admin/teams/${id}/members`, { userId }),
  adminUpdateTeamMemberRole: (id, userId, role) => apiFetch(`/admin/teams/${id}/members/${userId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ role }),
  }),
  adminRemoveTeamMember: (id, userId) => apiDelete(`/admin/teams/${id}/members/${userId}`),
  adminHideDirectoryUser: (userId) => apiDelete(`/admin/directory/${userId}`),
  adminUserMapping: (query = '') => apiFetch(`/admin/user-mapping${query ? `?q=${encodeURIComponent(query)}` : ''}`),
  adminSyncUserMapping: () => jsonPost('/admin/user-mapping/sync', {}),

  // Admin: projects
  adminProjects: () => apiFetch('/admin/projects'),
  adminCreateProject: (name, dashTeamId) => jsonPost('/admin/projects', { name, dashTeamId }),
  adminUpdateProject: (id, name, dashTeamId) => jsonPut(`/admin/projects/${id}`, { name, dashTeamId }),
  adminDeleteProject: (id) => apiDelete(`/admin/projects/${id}`),

  // Admin: project tasks
  adminAddProjectTask: (projectId, taskId) => jsonPost(`/admin/projects/${projectId}/tasks`, { taskId }),
  adminRemoveProjectTask: (projectId, taskId) => apiDelete(`/admin/projects/${projectId}/tasks/${taskId}`),

  // Task CRUD
  taskDetail: (id) => apiFetch(`/tasks/${id}`),
  taskUpdate: (id, body) => jsonPut(`/tasks/${id}`, body),
  taskSetStatus: (id, status) => apiFetch(`/tasks/${id}/status`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
  }),
  taskCompleteSelf: (id) => apiFetch(`/tasks/${id}/complete-self`, { method: 'POST' }),
  taskAddComment: (id, comment) => jsonPost(`/tasks/${id}/comments`, { comment }),
  taskCreate: (body) => jsonPost('/tasks', body),

  // Integrations
  integrations: () => apiFetch('/integrations'),
  integrationDetail: (id) => apiFetch(`/integrations/${id}`),
  integrationCreate: (body) => jsonPost('/integrations', body),
  integrationUpdate: (id, body) => jsonPut(`/integrations/${id}`, body),
  integrationDelete: (id) => apiDelete(`/integrations/${id}`),
  integrationTest: (id) => jsonPost(`/integrations/${id}/test`, {}),
  integrationRemoteFields: (id) => apiFetch(`/integrations/${id}/remote-fields`),
  integrationSaveMappings: (id, mappings) => jsonPut(`/integrations/${id}/mappings`, { mappings }),
  integrationSync: (id, direction) => jsonPost(`/integrations/${id}/sync`, { direction }),
  integrationLogs: (id) => apiFetch(`/integrations/${id}/logs`),
  localFields: () => apiFetch('/integrations/local-fields'),

  // CRM: Clients
  crmClients: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return crmFetch(`/clients${qs ? '?' + qs : ''}`);
  },
  crmClientDetail: (id) => crmFetch(`/clients/${id}`),
  crmCreateClient: (body) => crmPost('/clients', body),
  crmUpdateClient: (id, body) => crmPut(`/clients/${id}`, body),
  crmDeleteClient: (id) => crmDelete(`/clients/${id}`),

  // CRM: Pipeline summary
  crmPipelineSummary: () => crmFetch('/pipeline-summary'),
  crmMonthlySummary: (month) => crmFetch(`/monthly-summary${month ? '?month='+month : ''}`),
  crmYomiKanri: () => crmFetch('/yomi-kanri'),
  crmRoleTargets: () => crmFetch('/role-targets'),
  crmRoleTargetsSave: (targets) => crmFetch('/role-targets', { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({targets}) }),
  crmPeriodSettings: () => crmFetch('/period-settings'),
  crmPeriodSettingsSave: (body) => crmFetch('/period-settings', { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) }),
  crmIndividualPerformance: (staff) => crmFetch(`/individual-performance?staff=${encodeURIComponent(staff)}`),
  crmPerfStaff: () => crmFetch('/perf-staff'),

  // CRM: Deals
  crmDeals: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return crmFetch(`/deals${qs ? '?' + qs : ''}`);
  },
  crmDealDetail: (id) => crmFetch(`/deals/${id}`),
  crmCreateDeal: (body) => crmPost('/deals', body),
  crmUpdateDeal: (id, body) => crmFetch(`/deals/${id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  crmDeleteDeal: (id) => crmDelete(`/deals/${id}`),
  crmAddDealMember: (dealId, userId, role) => crmPost(`/deals/${dealId}/members`, { userId, role }),
  crmRemoveDealMember: (dealId, userId) => crmDelete(`/deals/${dealId}/members/${userId}`),

  // CRM: Deal full detail
  crmDealFull: (id) => crmFetch(`/deals/${id}/full`),

  // CRM: Activities
  crmActivities: (dealId) => crmFetch(`/deals/${dealId}/activities`),
  crmAddActivity: (dealId, body) => crmPost(`/deals/${dealId}/activities`, body),
  crmDeleteActivity: (dealId, actId) => crmDelete(`/deals/${dealId}/activities/${actId}`),

  // CRM: Activity Settings
  crmActivitySettings: () => crmFetch('/activity-settings'),
  crmSaveActivitySettings: (body) => crmFetch('/activity-settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),

  // CRM: Dashboard
  crmDashboard: (params = {}) => {
    const qs = new URLSearchParams(Object.fromEntries(Object.entries(params).filter(([,v]) => v != null && v !== ''))).toString();
    return crmFetch(`/dashboard${qs ? '?' + qs : ''}`);
  },
  crmDashboardDrilldown: (params) => {
    const qs = new URLSearchParams(params).toString();
    return crmFetch(`/dashboard/drilldown?${qs}`);
  },
  crmDashboardMonthlyTrend: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return crmFetch(`/dashboard/monthly-trend${qs ? '?' + qs : ''}`);
  },

  // CRM: Payments
  crmPayments: (dealId) => crmFetch(`/deals/${dealId}/payments`),
  crmAddPayment: (dealId, body) => crmPost(`/deals/${dealId}/payments`, body),
  crmUpdatePayment: (dealId, payId, body) => crmFetch(`/deals/${dealId}/payments/${payId}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }),
  crmDeletePayment: (dealId, payId) => crmDelete(`/deals/${dealId}/payments/${payId}`),

  // CRM: Deliverables
  crmDeliverables: (dealId) => crmFetch(`/deals/${dealId}/deliverables`),
  crmAddDeliverable: (dealId, body) => crmPost(`/deals/${dealId}/deliverables`, body),
  crmUpdateDeliverable: (dealId, dlvId, body) => crmFetch(`/deals/${dealId}/deliverables/${dlvId}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }),
  crmDeleteDeliverable: (dealId, dlvId) => crmFetch(`/deals/${dealId}/deliverables/${dlvId}`, { method: 'DELETE' }),

  // CRM: Deal Tasks
  crmDealTasks: (dealId) => crmFetch(`/deals/${dealId}/tasks`),
  crmAddDealTask: (dealId, taskId) => crmPost(`/deals/${dealId}/tasks`, { taskId }),
  crmRemoveDealTask: (dealId, taskId) => crmDelete(`/deals/${dealId}/tasks/${taskId}`),

  // CRM: Client Contacts
  crmClientContacts: (clientId) => crmFetch(`/clients/${clientId}/contacts`),
  crmAddClientContact: (clientId, body) => crmPost(`/clients/${clientId}/contacts`, body),
  crmUpdateClientContact: (clientId, cid, body) => crmFetch(`/clients/${clientId}/contacts/${cid}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }),
  crmDeleteClientContact: (clientId, cid) => crmFetch(`/clients/${clientId}/contacts/${cid}`, { method: 'DELETE' }),

  // CRM: Deal Positions
  crmDealPositions: (dealId) => crmFetch(`/deals/${dealId}/positions`),
  crmAddDealPosition: (dealId, body) => crmPost(`/deals/${dealId}/positions`, body),
  crmUpdateDealPosition: (dealId, posId, body) => crmFetch(`/deals/${dealId}/positions/${posId}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }),
  crmDeleteDealPosition: (dealId, posId) => crmFetch(`/deals/${dealId}/positions/${posId}`, { method: 'DELETE' }),

  // CRM: Deal Media Plans
  crmDealMediaPlans: (dealId) => crmFetch(`/deals/${dealId}/media-plans`),
  crmAddDealMediaPlan: (dealId, body) => crmPost(`/deals/${dealId}/media-plans`, body),
  crmUpdateDealMediaPlan: (dealId, planId, body) => crmFetch(`/deals/${dealId}/media-plans/${planId}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }),
  crmDeleteDealMediaPlan: (dealId, planId) => crmFetch(`/deals/${dealId}/media-plans/${planId}`, { method: 'DELETE' }),

  // CRM: Calc Defs
  crmCalcDefs: () => crmFetch('/calc-defs'),
  crmAddCalcDef: (body) => crmPost('/calc-defs', body),
  crmUpdateCalcDef: (defId, body) => crmFetch(`/calc-defs/${defId}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }),
  crmDeleteCalcDef: (defId) => crmFetch(`/calc-defs/${defId}`, { method: 'DELETE' }),

  // Analytics
  analyticsMemberCompletion: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return apiFetch(`/analytics/member-completion${qs ? '?' + qs : ''}`);
  },
  analyticsDueCompliance: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return apiFetch(`/analytics/due-compliance${qs ? '?' + qs : ''}`);
  },
  analyticsProjectProgress: () => apiFetch('/analytics/project-progress'),

  // kintone連携
  kintoneSearch:    (q)  => kintoneFetch(`/search?q=${encodeURIComponent(q)}`),
  kintoneRecord:    (id) => kintoneFetch(`/record/${encodeURIComponent(id)}`),

  // RPO案件管理
  rpoAccess:       ()        => rpoFetch('/access'),
  rpoClients:      (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return rpoFetch(`/clients${qs ? '?' + qs : ''}`);
  },
  rpoClient:       (id)      => rpoFetch(`/clients/${id}`),
  rpoCreateClient: (body)    => rpoPost('/clients', body),
  rpoUpdateClient: (id, body) => rpoFetch(`/clients/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }),
  rpoDeleteClient: (id)      => rpoFetch(`/clients/${id}`, { method: 'DELETE' }),
  rpoTeams:        ()        => rpoFetch('/teams'),
  rpoToggleHrTeam: (id, isHrDept) => rpoFetch(`/teams/${id}/hr`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ isHrDept }),
  }),

  // RPO: 案件タスク（workload items連携）
  rpoWorkloadItems:       (clientId)           => rpoFetch(`/clients/${clientId}/workload-items`),
  rpoCreateWorkloadItem:  (clientId, body)     => rpoPost(`/clients/${clientId}/workload-items`, body),
  rpoDeleteWorkloadItem:  (clientId, itemId)   => rpoFetch(`/clients/${clientId}/workload-items/${itemId}`, { method: 'DELETE' }),
  rpoMyTasks:       ()           => rpoFetch('/my-tasks'),
  rpoCreateMyTask:  (body)       => rpoPost('/my-tasks', body),
  rpoUpdateMyTask:  (id, patch)  => rpoFetch(`/my-tasks/${id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(patch) }),
  rpoDeleteMyTask:  (id)         => rpoFetch(`/my-tasks/${id}`, { method: 'DELETE' }),

  // RPO: タスクテンプレート
  rpoTemplates:          ()              => rpoFetch('/task-templates'),
  rpoCreateTemplate:     (body)          => rpoPost('/task-templates', body),
  rpoUpdateTemplate:     (id, body)      => rpoFetch(`/task-templates/${id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  rpoDeleteTemplate:     (id)            => rpoFetch(`/task-templates/${id}`, { method: 'DELETE' }),
  rpoApplyTemplates:     (clientId)      => rpoPost(`/clients/${clientId}/apply-templates`, {}),

  // RPO: 応募者
  rpoApplicants:         (clientId, params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return rpoFetch(`/clients/${clientId}/applicants${qs ? '?' + qs : ''}`);
  },
  rpoCreateApplicant:    (clientId, body)  => rpoPost(`/clients/${clientId}/applicants`, body),
  rpoUpdateApplicant:    (apId, body)      => rpoFetch(`/applicants/${apId}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  rpoDeleteApplicant:    (apId)            => rpoFetch(`/applicants/${apId}`, { method: 'DELETE' }),

  // RPO: 応募者アクション履歴
  rpoApplicantActions:   (apId)           => rpoFetch(`/applicants/${apId}/actions`),
  rpoAddApplicantAction: (apId, body)     => rpoPost(`/applicants/${apId}/actions`, body),

  // RPO: 求人媒体連携
  rpoMediaSources:       ()              => rpoFetch('/media-sources'),
  rpoCreateMediaSource:  (body)          => rpoPost('/media-sources', body),
  rpoUpdateMediaSource:  (id, body)      => rpoFetch(`/media-sources/${id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  rpoDeleteMediaSource:  (id)            => rpoFetch(`/media-sources/${id}`, { method: 'DELETE' }),

  // RPO: CSV一括インポート
  rpoImportCsv: (clientId, file) => {
    const form = new FormData();
    form.append('file', file);
    return rpoFetch(`/clients/${clientId}/import-csv`, { method: 'POST', body: form });
  },

  // RPO: 媒体マスタ
  rpoMediaMasters:       ()       => rpoFetch('/media-masters'),
  rpoCreateMediaMaster:  (name)   => rpoPost('/media-masters', { name }),
  rpoDeleteMediaMaster:  (id)     => rpoFetch(`/media-masters/${id}`, { method: 'DELETE' }),

  // RPO: 月次タスク生成
  rpoGenerateMonthlyTasks:       (yearMonth)           => rpoPost('/monthly-tasks/generate', { yearMonth }),
  rpoGenerateMonthlyTasksClient: (clientId, yearMonth) => rpoPost(`/clients/${clientId}/monthly-tasks/generate`, { yearMonth }),

  // RPO: 全案件サマリー
  rpoSummary: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return rpoFetch(`/summary${qs ? '?' + qs : ''}`);
  },

  // RPO: 設定
  rpoSettings:      ()       => rpoFetch('/settings'),
  rpoSaveSettings:  (body)   => rpoFetch('/settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),

  // RPO: スプシURL保存
  rpsSaveSheetUrl: (clientId, sheetsUrl) => rpoFetch(`/clients/${clientId}/sheets-url`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sheetsUrl }),
  }),

  // RPO: スプシ同期
  rpsSheetsSync: (clientId, sheetsUrl) => rpoFetch(`/clients/${clientId}/sheets-sync`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sheetsUrl }),
  }),

  // Google Drive
  driveFiles: (folderId) => driveFetch(`/files?folderId=${encodeURIComponent(folderId)}`),
  driveCandidates: (name) => rpoFetch(`/drive/candidates?name=${encodeURIComponent(name)}`),
  rpoAutoLinkSheets: (id, driveFolder) => rpoPost(`/clients/${id}/auto-link-sheets`, { driveFolder }),

  // RPO: 工数管理
  rpoUpdateWorkloadHours: (clientId, body) => rpoFetch(`/clients/${clientId}/workload-hours`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }),

  // 日報メンバー管理
  dailyReportMembers: () => apiFetch('/admin/daily-report/members'),
  dailyReportSync: () => apiFetch('/admin/daily-report/sync', { method: 'POST' }),
  dailyReportToggle: (userId, isTarget) => apiFetch(`/admin/daily-report/members/${userId}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ isTarget }),
  }),

  // Slackランキング
  rankingStart: (body) => apiFetch('/ranking/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  rankingStatus: (jobId) => apiFetch(`/ranking/status/${jobId}`),

  // CRM: 顧客管理（新）
  crmCustomers: (q, offset = 0, limit = 100, salesUser = '', yomiList = '') => crmFetch(`/customers?${new URLSearchParams({ ...(q ? { q } : {}), offset, limit, ...(salesUser ? { salesUser } : {}), ...(yomiList ? { yomiList } : {}) })}`),
  crmGetCustomer: (id) => crmFetch(`/customers/${id}`),
  crmCreateCustomer: (body) => crmPost('/customers', body),
  crmUpdateCustomer: (id, body) => crmFetch(`/customers/${id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  crmDeleteCustomer: (id) => crmDelete(`/customers/${id}`),

  taskCompleteSelf: (id) => apiFetch(`/tasks/${id}/complete-self`, { method: 'POST' }),

  workloadHours: (dashTeamId, from, to) => apiFetch(`/workload/hours?dashTeamId=${encodeURIComponent(dashTeamId)}${from ? `&from=${from}&to=${to}` : ''}`),

  // 権限管理
  permissionsGet: () => apiFetch('/admin/permissions'),
  permissionsSet: (body) => apiFetch('/admin/permissions', { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),

  // Slack グループ管理
  slackGroups: () => apiFetch('/admin/slack-groups'),
  slackGroupUpdateMember: (groupId, action, userId) =>
    apiFetch(`/admin/slack-groups/${groupId}/members`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action, userId }) }),

  // グループルール
  slackGroupRules: () => apiFetch('/admin/slack-group-rules'),
  // 採用管理
  recruitmentSettings: () => apiFetch('/admin/recruitment/settings'),
  recruitmentSettingsSave: (body) => apiFetch('/admin/recruitment/settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  recruitmentCandidates: () => apiFetch('/admin/recruitment/candidates'),
  recruitmentCandidateAdd: (body) => apiFetch('/admin/recruitment/candidates', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  recruitmentImportFromSheet: (spreadsheetUrl) => apiFetch('/admin/recruitment/import-sheet', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ spreadsheetUrl }) }),
  recruitmentCandidateDelete: (id) => apiFetch(`/admin/recruitment/candidates/${id}`, { method: 'DELETE' }),
  recruitmentSend: () => apiFetch('/admin/recruitment/send', { method: 'POST' }),
  recruitmentSendOne: (candidateId) => apiFetch('/admin/recruitment/send-one', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ candidateId }) }),

  slackGroupVisibility: () => apiFetch('/admin/slack-group-visibility'),
  slackGroupVisibilityUpdate: (groupId, hiddenTabs) => apiFetch(`/admin/slack-group-visibility/${groupId}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ hiddenTabs }) }),

  slackGroupRuleCreate: (body) => apiFetch('/admin/slack-group-rules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  slackGroupRuleUpdate: (id, body) => apiFetch(`/admin/slack-group-rules/${id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  // deptName 付きのショートハンド
  slackGroupRuleUpsert: (name, category, deptName, groupIds) => {
    // idが渡された場合はPUT, そうでなければPOST（フロントで判断）
    return apiFetch('/admin/slack-group-rules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, category, deptName, groupIds }) });
  },
  slackGroupRuleDelete: (id) => apiFetch(`/admin/slack-group-rules/${id}`, { method: 'DELETE' }),
  slackGroupRulesPreview: () => apiFetch('/admin/slack-group-rules/preview', { method: 'POST' }),
  slackGroupRulesApply: (changes) => apiFetch('/admin/slack-group-rules/apply', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ changes }) }),
  slackGroupChangePreview: (userId, toRuleId) => apiFetch('/admin/slack-group-rules/change-preview', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ userId, toRuleId }) }),
  slackGroupChangeApply: (userId, add, remove) => apiFetch('/admin/slack-group-rules/change-apply', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ userId, add, remove }) }),

  // チャンネルマッピング
  channelMappingSync: () => apiFetch('/admin/channel-mapping/sync', { method: 'POST' }),
  channelMappingSyncStatus: (jobId) => apiFetch(`/admin/channel-mapping/sync/status/${jobId}`),
  channelMappingStatus: () => apiFetch('/admin/channel-mapping/status'),
  channelMappingData: (type) => apiFetch(`/admin/channel-mapping/data${type ? `?type=${type}` : ''}`),
  channelMappingHidden: () => apiFetch('/admin/channel-mapping/hidden'),
  channelMappingHide: (id) => apiFetch(`/admin/channel-mapping/hidden/${id}`, { method: 'POST' }),
  channelMappingUnhide: (id) => apiFetch(`/admin/channel-mapping/hidden/${id}`, { method: 'DELETE' }),
  channelMappingUnhideAll: () => apiFetch('/admin/channel-mapping/hidden', { method: 'DELETE' }),
};
