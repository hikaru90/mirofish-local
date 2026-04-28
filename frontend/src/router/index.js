import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import Process from '../views/MainView.vue'
import SimulationView from '../views/SimulationView.vue'
import SimulationRunView from '../views/SimulationRunView.vue'
import ReportView from '../views/ReportView.vue'
import InteractionView from '../views/InteractionView.vue'

const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/:projectId/process',
    name: 'Process',
    component: Process,
    props: true
  },
  {
    path: '/:projectId/simulation',
    name: 'Simulation',
    component: SimulationView,
    props: route => ({
      projectId: route.params.projectId,
      simulationId: route.query.simulationId
    })
  },
  {
    path: '/:projectId/run',
    name: 'SimulationRun',
    component: SimulationRunView,
    props: route => ({
      projectId: route.params.projectId,
      simulationId: route.query.simulationId
    })
  },
  {
    path: '/:projectId/report',
    name: 'Report',
    component: ReportView,
    props: route => ({
      projectId: route.params.projectId,
      reportId: route.query.reportId
    })
  },
  {
    path: '/:projectId/interaction',
    name: 'Interaction',
    component: InteractionView,
    props: route => ({
      projectId: route.params.projectId,
      reportId: route.query.reportId
    })
  },
  // Legacy URLs -> canonical URLs
  {
    path: '/process/:projectId',
    redirect: to => ({ name: 'Process', params: { projectId: to.params.projectId } })
  },
  {
    path: '/simulation/:simulationId',
    redirect: to => ({
      name: 'Simulation',
      params: { projectId: to.query.projectId || 'unknown' },
      query: { simulationId: to.params.simulationId }
    })
  },
  {
    path: '/simulation/:simulationId/start',
    redirect: to => ({
      name: 'SimulationRun',
      params: { projectId: to.query.projectId || 'unknown' },
      query: { simulationId: to.params.simulationId, maxRounds: to.query.maxRounds }
    })
  },
  {
    path: '/report/:reportId',
    redirect: to => ({
      name: 'Report',
      params: { projectId: to.query.projectId || 'unknown' },
      query: { reportId: to.params.reportId }
    })
  },
  {
    path: '/interaction/:reportId',
    redirect: to => ({
      name: 'Interaction',
      params: { projectId: to.query.projectId || 'unknown' },
      query: { reportId: to.params.reportId }
    })
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
