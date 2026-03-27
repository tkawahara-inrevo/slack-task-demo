const BASE = '/api/dashboard';

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

export const api = {
  // General
  me: () => apiFetch('/me'),
  summary: () => apiFetch('/summary'),
  tasks: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return apiFetch(`/tasks?${qs}`);
  },
  members: () => apiFetch('/members'),
  overdue: () => apiFetch('/overdue'),
  myTeams: () => apiFetch('/my-teams'),
  projects: () => apiFetch('/projects'),
  projectTasks: (id) => apiFetch(`/projects/${id}/tasks`),

  // Admin: roles
  adminRoles: () => apiFetch('/admin/roles'),
  adminSetRole: (userId, role) => jsonPost('/admin/roles', { userId, role }),

  // Admin: teams
  adminTeams: () => apiFetch('/admin/teams'),
  adminCreateTeam: (name) => jsonPost('/admin/teams', { name }),
  adminUpdateTeam: (id, name) => jsonPut(`/admin/teams/${id}`, { name }),
  adminDeleteTeam: (id) => apiDelete(`/admin/teams/${id}`),

  // Admin: team members
  adminTeamMembers: (id) => apiFetch(`/admin/teams/${id}/members`),
  adminAddTeamMember: (id, userId) => jsonPost(`/admin/teams/${id}/members`, { userId }),
  adminRemoveTeamMember: (id, userId) => apiDelete(`/admin/teams/${id}/members/${userId}`),

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

  // Analytics
  analyticsMemberCompletion: () => apiFetch('/analytics/member-completion'),
  analyticsDueCompliance: () => apiFetch('/analytics/due-compliance'),
  analyticsProjectProgress: () => apiFetch('/analytics/project-progress'),
};
