<template>
  <a-menu
    :selected-keys="[currentKey]"
    :auto-open-selected="true"
    @menu-item-click="onMenuClick"
  >
    <a-menu-item key="dashboard">
      <template #icon><icon-dashboard /></template>
      仪表盘
    </a-menu-item>

    <a-sub-menu key="task-group">
      <template #icon><icon-thunderbolt /></template>
      <template #title>原子任务</template>
      <a-menu-item key="tasks">任务模板</a-menu-item>
      <a-menu-item key="task-instances">任务实例</a-menu-item>
    </a-sub-menu>

    <a-sub-menu key="workflow-group">
      <template #icon><icon-branch /></template>
      <template #title>工作流</template>
      <a-menu-item key="workflows">工作流管理</a-menu-item>
      <a-menu-item key="workflow-instances">工作流实例</a-menu-item>
    </a-sub-menu>

    <a-menu-item key="variables">
      <template #icon><icon-lock /></template>
      变量管理
    </a-menu-item>

    <a-menu-item key="approvals">
      <template #icon><icon-check-square /></template>
      审批中心
    </a-menu-item>

    <a-menu-item key="notifications">
      <template #icon><icon-notification /></template>
      通知中心
    </a-menu-item>

    <a-menu-item key="subscriptions">
      <template #icon><icon-settings /></template>
      通知订阅
    </a-menu-item>

    <a-menu-item v-if="canManageUsers" key="users">
      <template #icon><icon-user-group /></template>
      用户管理
    </a-menu-item>

    <a-menu-item v-if="canManageUsers" key="api-keys">
      <template #icon><icon-safe /></template>
      API Keys
    </a-menu-item>

    <a-menu-item v-if="isSuperAdmin" key="tenants">
      <template #icon><icon-apps /></template>
      租户管理
    </a-menu-item>
  </a-menu>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { usePermission } from '../../composables/use-permission'
import {
  IconDashboard,
  IconThunderbolt,
  IconBranch,
  IconLock,
  IconCheckSquare,
  IconNotification,
  IconSettings,
  IconUserGroup,
  IconApps,
  IconSafe,
} from '@arco-design/web-vue/es/icon'

const router = useRouter()
const route = useRoute()
const { canManageUsers, isSuperAdmin } = usePermission()

const MENU_ROUTE_MAP: Record<string, string> = {
  dashboard: '/',
  tasks: '/tasks',
  'task-instances': '/tasks/instances',
  workflows: '/workflows',
  'workflow-instances': '/workflows/instances',
  variables: '/variables',
  approvals: '/approvals',
  notifications: '/notifications',
  subscriptions: '/subscriptions',
  users: '/users',
  'api-keys': '/api-keys',
  tenants: '/tenants',
}

const ROUTE_MENU_MAP: Record<string, string> = {
  '/': 'dashboard',
  '/tasks': 'tasks',
  '/tasks/instances': 'task-instances',
  '/workflows': 'workflows',
  '/workflows/instances': 'workflow-instances',
  '/variables': 'variables',
  '/approvals': 'approvals',
  '/notifications': 'notifications',
  '/subscriptions': 'subscriptions',
  '/users': 'users',
  '/api-keys': 'api-keys',
  '/tenants': 'tenants',
}

const currentKey = computed(() => {
  const path = route.path
  for (const [prefix, key] of Object.entries(ROUTE_MENU_MAP)) {
    if (path === prefix || (prefix !== '/' && path.startsWith(prefix))) return key
  }
  return 'dashboard'
})

function onMenuClick(key: string) {
  const target = MENU_ROUTE_MAP[key]
  if (target) router.push(target)
}
</script>
