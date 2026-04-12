const BASE = '/api/dashboard';
const CRM = '/api/crm';

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
  if (!res.ok) throw new Error(`API error: ${res.status}`);
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
  adminCreateTeam: (name, parentId = null) => jsonPost('/admin/teams', { name, parentId }),
  adminUpdateTeam: (id, name) => jsonPut(`/admin/teams/${id}`, { name }),
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

  // CRM: Deals
  crmDeals: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return crmFetch(`/deals${qs ? '?' + qs : ''}`);
  },
  crmDealDetail: (id) => crmFetch(`/deals/${id}`),
  crmCreateDeal: (body) => crmPost('/deals', body),
  crmUpdateDeal: (id, body) => crmPut(`/deals/${id}`, body),
  crmDeleteDeal: (id) => crmDelete(`/deals/${id}`),
  crmAddDealMember: (dealId, userId, role) => crmPost(`/deals/${dealId}/members`, { userId, role }),
  crmRemoveDealMember: (dealId, userId) => crmDelete(`/deals/${dealId}/members/${userId}`),

  // CRM: Deal full detail
  crmDealFull: (id) => crmFetch(`/deals/${id}/full`),

  // CRM: Activities
  crmActivities: (dealId) => crmFetch(`/deals/${dealId}/activities`),
  crmAddActivity: (dealId, body) => crmPost(`/deals/${dealId}/activities`, body),
  crmDeleteActivity: (dealId, actId) => crmDelete(`/deals/${dealId}/activities/${actId}`),

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
};
