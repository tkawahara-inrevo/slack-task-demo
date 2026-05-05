import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import FloatingTasks from './pages/FloatingTasks'
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
import DailyReportAdmin from './pages/admin/DailyReportAdmin'
import ChannelMapping from './pages/admin/ChannelMapping'
import PermissionsManager from './pages/admin/PermissionsManager'
import SlackGroups from './pages/admin/SlackGroups'
import Recruitment from './pages/admin/Recruitment'
import ProjectView from './pages/ProjectView'
import Analytics from './pages/Analytics'
import WorkloadGantt from './pages/WorkloadGantt'
import OrgChart from './pages/OrgChart'
import Ranking from './pages/Ranking'
import ClientList from './pages/rpo/ClientList'
import ClientDetail from './pages/rpo/ClientDetail'
import RpoSummary from './pages/rpo/RpoSummary'
import RpoWorkload from './pages/rpo/RpoWorkload'
import RpoMyTasks from './pages/rpo/RpoMyTasks'
import CRM from './pages/crm/CRM'
import CustomerDetail from './pages/crm/CustomerDetail'
import Pipeline from './pages/crm/Pipeline'
import CustomerList from './pages/crm/CustomerList'
import SalesPerformance from './pages/crm/SalesPerformance'
import './App.css'

function App() {
  return (
    <Routes>
      <Route path="/floating" element={<FloatingTasks />} />
      <Route path="*" element={
        <Layout>
          <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/tasks/new" element={<TaskCreate />} />
        <Route path="/tasks/:id" element={<TaskDetail />} />
        <Route path="/projects/:id" element={<ProjectView />} />
        <Route path="/analytics" element={<Analytics />} />
        <Route path="/workload" element={<WorkloadGantt />} />
        <Route path="/org-chart" element={<OrgChart />} />
        <Route path="/ranking" element={<Ranking />} />
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
          <Route path="daily-report" element={<DailyReportAdmin />} />
          <Route path="permissions-manager" element={<PermissionsManager />} />
          <Route path="channel-mapping" element={<ChannelMapping />} />
          <Route path="slack-groups" element={<SlackGroups />} />
          <Route path="recruitment" element={<Recruitment />} />
          <Route path="ranking" element={<Ranking />} />
        </Route>
        <Route path="/crm" element={<CRM />} />
        <Route path="/crm/customers/:id" element={<CustomerDetail />} />
        {/* 旧URLの後方互換 */}
        <Route path="/crm/pipeline" element={<Pipeline />} />
        <Route path="/crm/customers" element={<CustomerList />} />
        <Route path="/rpo" element={<ClientList />} />
        <Route path="/rpo/summary" element={<RpoSummary />} />
        <Route path="/rpo/workload" element={<RpoWorkload />} />
        <Route path="/rpo/mytasks" element={<RpoMyTasks />} />
        <Route path="/rpo/:id" element={<ClientDetail />} />
        <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Layout>
      } />
    </Routes>
  )
}

export default App
