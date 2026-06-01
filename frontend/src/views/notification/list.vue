<template>
  <div class="notification-list-page">
    <div class="page-header">
      <h3>通知中心</h3>
      <a-button type="outline" size="small" @click="handleMarkAllRead" :loading="markingAll">
        全部已读
      </a-button>
    </div>
    <a-spin :loading="loading" style="width:100%">
      <a-list v-if="notifications.length" :bordered="false" :split="true">
        <a-list-item
          v-for="item in notifications"
          :key="item.notification_id"
          class="notif-item"
          :class="{ unread: !item.read }"
          @click="openNotification(item)"
        >
          <a-list-item-meta>
            <template #title>
              <span class="notif-title">
                <a-tag :color="statusColor(item.event_type)" size="small">{{ eventLabel(item.event_type) }}</a-tag>
                <span v-if="!item.read" class="unread-dot" />
              </span>
            </template>
            <template #description>
              <span class="notif-time">{{ formatTime(item.created_at) }}</span>
            </template>
          </a-list-item-meta>
        </a-list-item>
      </a-list>
      <a-empty v-else description="暂无通知" />
    </a-spin>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { notificationApi } from '../../api/notification'
import type { NotificationRecord } from '../../types/notification'
import { Notification } from '@arco-design/web-vue'

const router = useRouter()
const notifications = ref<NotificationRecord[]>([])
const loading = ref(false)
const markingAll = ref(false)

onMounted(() => {
  fetchNotifications()
})

async function fetchNotifications() {
  loading.value = true
  try {
    const res = await notificationApi.list(1, 50)
    notifications.value = res.data || []
  } catch { /* ignore */ } finally { loading.value = false }
}

async function handleMarkAllRead() {
  markingAll.value = true
  try {
    await notificationApi.markAllRead()
    notifications.value.forEach(n => (n.read = true))
    Notification.success({ content: '已全部标记为已读' })
  } catch {} finally { markingAll.value = false }
}

function openNotification(item: NotificationRecord) {
  if (!item.read) {
    notificationApi.markRead(item.notification_id).catch(() => {})
    item.read = true
  }
  if (item.url) {
    const path = item.url.replace(/^https?:\/\/[^/]+/, '')
    if (path) router.push(path)
  }
}

function eventLabel(type: string): string {
  const map: Record<string, string> = {
    'workflow.started': '工作流启动',
    'workflow.completed': '已完成',
    'workflow.failed': '执行失败',
    'workflow.canceled': '已取消',
    'node.success': '节点成功',
    'node.failed': '节点失败',
    'node.skipped': '已跳过',
    'approval.pending': '待审批',
    'approval.approved': '审批通过',
    'approval.rejected': '审批驳回',
    'approval.expired': '审批过期',
    'pause.expired': '暂停到期',
    'sweeper.recovered': '实例恢复',
  }
  return map[type] || type
}

function statusColor(type: string): string {
  if (type.includes('failed') || type.includes('rejected') || type.includes('expired')) return 'red'
  if (type.includes('completed') || type.includes('success') || type.includes('approved')) return 'green'
  if (type.includes('pending') || type.includes('started')) return 'blue'
  return 'gray'
}

function formatTime(ts: string): string {
  if (!ts) return ''
  const d = new Date(ts)
  const now = new Date()
  const diff = now.getTime() - d.getTime()
  if (diff < 60000) return '刚刚'
  if (diff < 3600000) return `${Math.floor(diff / 60000)} 分钟前`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)} 小时前`
  return d.toLocaleDateString() + ' ' + d.toLocaleTimeString()
}
</script>

<style scoped>
.notification-list-page {
  max-width: 800px;
  margin: 0 auto;
}
.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
.notif-item {
  cursor: pointer;
  padding: 12px 0;
}
.notif-item.unread {
  background: var(--color-fill-1);
}
.notif-title {
  display: flex;
  align-items: center;
  gap: 8px;
}
.unread-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-primary-6);
}
.notif-time {
  color: var(--color-text-3);
  font-size: 12px;
}
</style>
