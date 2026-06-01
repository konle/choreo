<template>
  <div class="header-bar">
    <div class="header-left">
      <a-breadcrumb>
        <a-breadcrumb-item>{{ currentRouteName }}</a-breadcrumb-item>
      </a-breadcrumb>
    </div>
    <div class="header-right">
      <a-select
        v-if="isSuperAdmin"
        v-model="currentTenant"
        placeholder="选择租户"
        style="width: 200px; margin-right: 16px"
        @change="onTenantChange"
      >
        <a-option
          v-for="t in tenants"
          :key="t.tenant_id"
          :value="t.tenant_id"
          :label="t.name"
        />
      </a-select>

      <a-popover trigger="click" @popup-visible-change="onNotifPopup">
        <a-badge :count="unreadCount" :max-count="99">
          <a-button type="text">
            <template #icon><icon-notification /></template>
          </a-button>
        </a-badge>
        <template #content>
          <div class="notif-popup">
            <div v-if="recentNotifications.length === 0 && !notifLoading" style="padding: 12px; text-align: center; color: var(--color-text-3);">
              暂无通知
            </div>
            <a-list v-else :bordered="false" style="width: 320px;">
              <a-list-item
                v-for="item in recentNotifications.slice(0, 5)"
                :key="item.notification_id"
                class="notif-popup-item"
                @click="openNotif(item)"
              >
                <a-list-item-meta>
                  <template #title>
                    <span style="font-size:13px;">
                      <a-tag :color="notifColor(item.event_type)" size="small">{{ notifLabel(item.event_type) }}</a-tag>
                    </span>
                  </template>
                  <template #description>
                    <span style="font-size:12px;color:var(--color-text-3);">{{ notifTime(item.created_at) }}</span>
                  </template>
                </a-list-item-meta>
              </a-list-item>
            </a-list>
            <div style="text-align:center;padding:8px;border-top:1px solid var(--color-border-1);">
              <a-button type="text" size="mini" @click="goNotifications">查看全部</a-button>
            </div>
          </div>
        </template>
      </a-popover>

      <a-dropdown>
        <a-button type="text">
          <template #icon><icon-user /></template>
          {{ auth.username }}
        </a-button>
        <template #content>
          <a-doption @click="showProfile = true">
            <template #icon><icon-idcard /></template>
            个人信息
          </a-doption>
          <a-doption @click="showChangePw = true">
            <template #icon><icon-lock /></template>
            修改密码
          </a-doption>
          <a-divider style="margin: 4px 0" />
          <a-doption @click="handleLogout">
            <template #icon><icon-export /></template>
            退出登录
          </a-doption>
        </template>
      </a-dropdown>
    </div>

    <!-- 个人信息 -->
    <a-modal v-model:visible="showProfile" title="个人信息" :footer="false" :width="400">
      <a-descriptions :column="1" bordered size="medium" v-if="profile">
        <a-descriptions-item label="用户名">{{ profile.username }}</a-descriptions-item>
        <a-descriptions-item label="邮箱">{{ profile.email }}</a-descriptions-item>
        <a-descriptions-item label="角色">{{ auth.isSuperAdmin ? '超级管理员' : auth.role }}</a-descriptions-item>
        <a-descriptions-item label="状态">{{ profile.status }}</a-descriptions-item>
        <a-descriptions-item label="注册时间">{{ profile.created_at }}</a-descriptions-item>
      </a-descriptions>
      <a-spin v-else style="display: block; text-align: center; padding: 24px" />
    </a-modal>

    <!-- 修改密码 -->
    <a-modal v-model:visible="showChangePw" title="修改密码" @ok="handleChangePassword" :ok-loading="changePwLoading" :width="400">
      <a-form :model="pwForm" layout="vertical">
        <a-form-item label="当前密码" required>
          <a-input-password v-model="pwForm.old_password" placeholder="请输入当前密码" />
        </a-form-item>
        <a-form-item label="新密码" required>
          <a-input-password v-model="pwForm.new_password" placeholder="至少6位" />
        </a-form-item>
        <a-form-item label="确认新密码" required>
          <a-input-password v-model="pwForm.confirm_password" placeholder="再次输入新密码" />
        </a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted, computed, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useAuthStore } from '../../stores/auth'
import { usePermission } from '../../composables/use-permission'
import { tenantApi } from '../../api/tenant'
import { authApi } from '../../api/auth'
import { IconUser, IconLock, IconExport, IconIdcard, IconNotification } from '@arco-design/web-vue/es/icon'
import { Notification } from '@arco-design/web-vue'
import type { TenantEntity } from '../../types/tenant'
import type { UserProfile } from '../../types/auth'
import { notificationApi } from '../../api/notification'
import { usePolling } from '../../composables/use-polling'
import type { NotificationRecord } from '../../types/notification'

const router = useRouter()
const route = useRoute()
const auth = useAuthStore()
const { isSuperAdmin } = usePermission()

const tenants = ref<TenantEntity[]>([])
const currentTenant = ref(auth.tenantId)
const showProfile = ref(false)
const showChangePw = ref(false)
const changePwLoading = ref(false)
const profile = ref<UserProfile | null>(null)
const pwForm = reactive({ old_password: '', new_password: '', confirm_password: '' })
const unreadCount = ref(0)
const recentNotifications = ref<NotificationRecord[]>([])
const notifLoading = ref(false)

const { start: startPolling } = usePolling(async () => {
  try {
    const res = await notificationApi.unreadCount()
    unreadCount.value = res.data ?? 0
  } catch { /* ignore */ }
}, 30000)

const currentRouteName = computed(() => {
  return (route.meta?.title as string) || route.name?.toString() || ''
})

onMounted(async () => {
  if (isSuperAdmin.value) {
    try {
      const res = await tenantApi.list()
      tenants.value = res.data
    } catch { /* ignore */ }
  }
  startPolling()
  try {
    const res = await notificationApi.unreadCount()
    unreadCount.value = res.data ?? 0
  } catch { /* ignore */ }
})

watch(showProfile, async (val) => {
  if (val && !profile.value) {
    try {
      const res = await authApi.getProfile()
      profile.value = res.data
    } catch { /* ignore */ }
  }
})

function onTenantChange(val: string | number | boolean | Record<string, any> | (string | number | boolean | Record<string, any>)[]) {
  auth.switchTenant(val as string)
  router.go(0)
}

async function handleChangePassword() {
  if (pwForm.new_password !== pwForm.confirm_password) {
    Notification.error({ content: '两次输入的密码不一致' })
    return
  }
  if (pwForm.new_password.length < 6) {
    Notification.error({ content: '新密码至少6位' })
    return
  }
  changePwLoading.value = true
  try {
    await authApi.changePassword({
      old_password: pwForm.old_password,
      new_password: pwForm.new_password,
    })
    Notification.success({ content: '密码修改成功，请重新登录' })
    showChangePw.value = false
    pwForm.old_password = ''
    pwForm.new_password = ''
    pwForm.confirm_password = ''
    auth.logout()
    router.push('/login')
  } catch {} finally { changePwLoading.value = false }
}

function handleLogout() {
  auth.logout()
  router.push('/login')
}

async function onNotifPopup(visible: boolean) {
  if (visible) {
    notifLoading.value = true
    try {
      const res = await notificationApi.list(1, 5)
      recentNotifications.value = res.data || []
    } catch {} finally { notifLoading.value = false }
  }
}

function openNotif(item: NotificationRecord) {
  if (!item.read) {
    notificationApi.markRead(item.notification_id).catch(() => {})
    item.read = true
    unreadCount.value = Math.max(0, unreadCount.value - 1)
  }
  if (item.url) {
    const path = item.url.replace(/^https?:\/\/[^/]+/, '')
    if (path) router.push(path)
  }
}

function goNotifications() {
  router.push('/notifications')
}

function notifLabel(type: string): string {
  const map: Record<string, string> = {
    'workflow.started': '启动', 'workflow.completed': '已完成',
    'workflow.failed': '失败', 'workflow.canceled': '已取消',
    'node.success': '节点成功', 'node.failed': '节点失败',
    'approval.pending': '待审批', 'approval.approved': '审批通过',
    'approval.rejected': '已驳回', 'sweeper.recovered': '已恢复',
  }
  return map[type] || type
}

function notifColor(type: string): string {
  if (type.includes('failed') || type.includes('rejected')) return 'red'
  if (type.includes('completed') || type.includes('success') || type.includes('approved')) return 'green'
  if (type.includes('pending') || type.includes('started')) return 'blue'
  return 'gray'
}

function notifTime(ts: string): string {
  if (!ts) return ''
  const d = new Date(ts)
  const now = new Date()
  const diff = now.getTime() - d.getTime()
  if (diff < 60000) return '刚刚'
  if (diff < 3600000) return `${Math.floor(diff / 60000)} 分钟前`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)} 小时前`
  return d.toLocaleDateString()
}
</script>

<style scoped>
.header-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}
.header-right {
  display: flex;
  align-items: center;
  gap: 8px;
}
</style>
