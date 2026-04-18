import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import TaskDetail from './pages/TaskDetail'
import TaskCreate from './pages/TaskCreate'
import Unauthorized from './pages/Unauthorized'
import AdminLayout from './pages/admin/AdminLayout'
import TeamsAdmin from './pages/admin/TeamsAdmin'
import ProjectsAdmin from './pages/admin/ProjectsAdmin'
import RolesAdmin from './pages/admin/RolesAdmin'
import PermissionsAdmin from './pages/admin/PermissionsAdmin'
import IntegrationsAdmin from './pages/admin/IntegrationsAdmin'
import FormulasAdmin from './pages/admin/FormulasAdmin'
import UserMappingAdmin from './pages/admin/UserMappingAdmin'
import RpoAdmin from './pages/admin/RpoAdmin'
import ProjectView from './pages/ProjectView'
import Analytics from './pages/Analytics'
import WorkloadGantt from './pages/WorkloadGantt'
import OrgChart from './pages/OrgChart'
import ClientList from './pages/rpo/ClientList'
import ClientDetail from './pages/rpo/ClientDetail'
import TaskTreeView from './pages/rpo/TaskTreeView'
import RpoSummary from './pages/rpo/RpoSummary'
import RpoWorkload from './pages/rpo/RpoWorkload'
import './App.css'

function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/tasks/new" element={<TaskCreate />} />
        <Route path="/tasks/:id" element={<TaskDetail />} />
        <Route path="/projects/:id" element={<ProjectView />} />
        <Route path="/analytics" element={<Analytics />} />
        <Route path="/workload" element={<WorkloadGantt />} />
        <Route path="/org-chart" element={<OrgChart />} />
        <Route path="/unauthorized" element={<Unauthorized />} />
        <Route path="/admin" element={<AdminLayout />}>
          <Route index element={<Navigate to="roles" replace />} />
          <Route path="teams" element={<TeamsAdmin />} />
          <Route path="projects" element={<ProjectsAdmin />} />
          <Route path="roles" element={<RolesAdmin />} />
          <Route path="permissions" element={<PermissionsAdmin />} />
          <Route path="user-mapping" element={<UserMappingAdmin />} />
          <Route path="integrations" element={<IntegrationsAdmin />} />
          <Route path="formulas" element={<FormulasAdmin />} />
          <Route path="rpo" element={<RpoAdmin />} />
        </Route>
        <Route path="/rpo" element={<ClientList />} />
        <Route path="/rpo/summary" element={<RpoSummary />} />
        <Route path="/rpo/workload" element={<RpoWorkload />} />
        <Route path="/rpo/tree" element={<TaskTreeView />} />
        <Route path="/rpo/:id" element={<ClientDetail />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  )
}

export default App
