import { createRouter, createWebHistory } from 'vue-router'
import type { RouteRecordRaw } from 'vue-router'

const routes: RouteRecordRaw[] = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('../views/auth/login.vue'),
    meta: { public: true },
  },
  {
    path: '/register',
    name: 'Register',
    component: () => import('../views/auth/register.vue'),
    meta: { public: true },
  },
  {
    path: '/',
    component: () => import('../components/layout/app-layout.vue'),
    children: [
      {
        path: '',
        name: 'Dashboard',
        component: () => import('../views/dashboard/index.vue'),
      },
      {
        path: 'tenants',
        name: 'TenantList',
        component: () => import('../views/tenant/list.vue'),
        meta: { permission: 'TenantManage' },
      },
      {
        path: 'tenants/:id',
        name: 'TenantDetail',
        component: () => import('../views/tenant/detail.vue'),
        meta: { permission: 'TenantManage' },
      },
      {
        path: 'users',
        name: 'UserList',
        component: () => import('../views/user/list.vue'),
        meta: { permission: 'UserManage' },
      },
      {
        path: 'tasks',
        name: 'TaskTemplateList',
        component: () => import('../views/task/template/list.vue'),
      },
      {
        path: 'tasks/create',
        name: 'TaskTemplateCreate',
        component: () => import('../views/task/template/editor.vue'),
        meta: { permission: 'TemplateWrite' },
      },
      {
        path: 'tasks/:id/edit',
        name: 'TaskTemplateEdit',
        component: () => import('../views/task/template/editor.vue'),
        meta: { permission: 'TemplateWrite' },
      },
      {
        path: 'tasks/instances',
        name: 'TaskInstanceList',
        component: () => import('../views/task/instance/list.vue'),
      },
      {
        path: 'tasks/instances/:id',
        name: 'TaskInstanceDetail',
        component: () => import('../views/task/instance/detail.vue'),
      },
      {
        path: 'workflows',
        name: 'WorkflowMetaList',
        component: () => import('../views/workflow/meta/list.vue'),
      },
      {
        path: 'workflows/create',
        name: 'WorkflowMetaCreate',
        component: () => import('../views/workflow/meta/editor.vue'),
        meta: { permission: 'TemplateWrite' },
      },
      {
        path: 'workflows/:metaId',
        name: 'WorkflowMetaDetail',
        component: () => import('../views/workflow/meta/detail.vue'),
      },
      {
        path: 'workflows/:metaId/editor',
        name: 'WorkflowEditorNew',
        component: () => import('../views/workflow/editor/index.vue'),
        meta: { permission: 'TemplateWrite' },
      },
      {
        path: 'workflows/:metaId/editor/:version',
        name: 'WorkflowEditorVersion',
        component: () => import('../views/workflow/editor/index.vue'),
        meta: { permission: 'TemplateWrite' },
      },
      {
        path: 'workflows/instances',
        name: 'WorkflowInstanceList',
        component: () => import('../views/workflow/instance/list.vue'),
      },
      {
        path: 'workflows/instances/:id',
        name: 'WorkflowInstanceDetail',
        component: () => import('../views/workflow/instance/detail.vue'),
      },
      {
        path: 'variables',
        name: 'VariableList',
        component: () => import('../views/variable/tenant-list.vue'),
      },
      {
        path: 'approvals',
        name: 'ApprovalList',
        component: () => import('../views/approval/list.vue'),
      },
      {
        path: 'approvals/:id',
        name: 'ApprovalDetail',
        component: () => import('../views/approval/detail.vue'),
      },
      {
        path: 'api-keys',
        name: 'ApiKeyList',
        component: () => import('../views/apikey/list.vue'),
        meta: { permission: 'UserManage' },
      },
      {
        path: 'notifications',
        name: 'NotificationList',
        component: () => import('../views/notification/list.vue'),
        meta: { title: '通知中心' },
      },
      {
        path: 'subscriptions',
        name: 'SubscriptionList',
        component: () => import('../views/subscription/list.vue'),
        meta: { title: '通知订阅' },
      },
    ],
  },
  {
    path: '/403',
    name: 'Forbidden',
    component: () => import('../views/error/403.vue'),
    meta: { public: true },
  },
  {
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: () => import('../views/error/404.vue'),
    meta: { public: true },
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
