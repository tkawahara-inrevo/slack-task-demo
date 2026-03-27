import { Routes, Route, Navigate } from 'react-router-dom'
import Dashboard from './pages/Dashboard'
import TaskDetail from './pages/TaskDetail'
import TaskCreate from './pages/TaskCreate'
import Unauthorized from './pages/Unauthorized'
import AdminLayout from './pages/admin/AdminLayout'
import TeamsAdmin from './pages/admin/TeamsAdmin'
import ProjectsAdmin from './pages/admin/ProjectsAdmin'
import RolesAdmin from './pages/admin/RolesAdmin'
import IntegrationsAdmin from './pages/admin/IntegrationsAdmin'
import ProjectView from './pages/ProjectView'
import Analytics from './pages/Analytics'
import './App.css'

function App() {
  return (
    <Routes>
      <Route path="/" element={<Dashboard />} />
      <Route path="/tasks/new" element={<TaskCreate />} />
      <Route path="/tasks/:id" element={<TaskDetail />} />
      <Route path="/projects/:id" element={<ProjectView />} />
      <Route path="/analytics" element={<Analytics />} />
      <Route path="/unauthorized" element={<Unauthorized />} />
      <Route path="/admin" element={<AdminLayout />}>
        <Route index element={<Navigate to="teams" replace />} />
        <Route path="teams" element={<TeamsAdmin />} />
        <Route path="projects" element={<ProjectsAdmin />} />
        <Route path="roles" element={<RolesAdmin />} />
        <Route path="integrations" element={<IntegrationsAdmin />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}

export default App
