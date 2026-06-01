<template>
  <div class="subscription-page">
    <div class="page-header">
      <h3>通知订阅</h3>
      <a-button type="primary" size="small" @click="openCreate">新建订阅</a-button>
    </div>
    <a-spin :loading="loading">
      <a-list v-if="subs.length" :bordered="false" :split="true">
        <a-list-item v-for="s in subs" :key="s.subscription_id">
          <a-list-item-meta>
            <template #title>
              <span>
                <a-tag :color="s.scope === 'Global' ? 'blue' : 'purple'" size="small">
                  {{ s.scope === 'Global' ? '全局' : '资源级' }}
                </a-tag>
                <span v-if="s.scope === 'Resource' && s.resource_id" style="margin-left:8px;font-size:13px;">
                  模板: {{ s.resource_id }}
                </span>
                <a-switch
                  :model-value="s.enabled"
                  size="small"
                  style="margin-left:12px"
                  @change="(v: string | number | boolean) => toggleEnabled(s, Boolean(v))"
                />
              </span>
            </template>
            <template #description>
              <span style="font-size:12px;color:var(--color-text-3);">
                事件: {{ s.event_types.map(e => eventLabel(e)).join(', ') }}
                &nbsp;|&nbsp; 渠道: {{ s.channels.map(c => c.type === 'InApp' ? '站内' : 'Webhook').join(', ') }}
              </span>
            </template>
          </a-list-item-meta>
          <template #actions>
            <a-button type="text" size="mini" @click="openEdit(s)">编辑</a-button>
            <a-popconfirm content="确定删除？" @ok="handleDelete(s.subscription_id)">
              <a-button type="text" size="mini" status="danger">删除</a-button>
            </a-popconfirm>
          </template>
        </a-list-item>
      </a-list>
      <a-empty v-else description="暂无订阅" />
    </a-spin>

    <!-- 编辑/创建弹窗 -->
    <a-modal
      v-model:visible="modalVisible"
      :title="editing ? '编辑订阅' : '新建订阅'"
      @ok="handleSave"
      :ok-loading="saving"
      :width="520"
    >
      <a-form :model="form" layout="vertical">
        <a-form-item label="范围">
          <a-radio-group v-model="form.scope">
            <a-radio value="global">全局（租户级）</a-radio>
            <a-radio value="resource">资源级（指定模板）</a-radio>
          </a-radio-group>
        </a-form-item>
        <a-form-item v-if="form.scope === 'resource'" label="模板 ID">
          <a-input v-model="form.resource_id" placeholder="workflow_meta_id" />
        </a-form-item>
        <a-form-item label="事件类型">
          <a-checkbox-group v-model="form.event_types" direction="vertical">
            <a-checkbox value="workflow.started">工作流启动</a-checkbox>
            <a-checkbox value="workflow.completed">工作流完成</a-checkbox>
            <a-checkbox value="workflow.failed">工作流失败</a-checkbox>
            <a-checkbox value="workflow.canceled">工作流取消</a-checkbox>
            <a-checkbox value="node.success">节点成功</a-checkbox>
            <a-checkbox value="node.failed">节点失败</a-checkbox>
            <a-checkbox value="node.skipped">节点跳过</a-checkbox>
          </a-checkbox-group>
        </a-form-item>
        <a-form-item label="通知渠道">
          <a-checkbox-group v-model="selectedChannels" direction="vertical">
            <a-checkbox value="InApp">站内通知</a-checkbox>
            <a-checkbox value="Webhook">Webhook</a-checkbox>
          </a-checkbox-group>
        </a-form-item>
        <a-form-item v-if="selectedChannels.includes('Webhook')" label="Webhook URL">
          <a-input v-model="form.webhook_url" placeholder="https://your-domain.com/hook" />
        </a-form-item>
        <a-form-item v-if="selectedChannels.includes('Webhook')" label="Secret (可选)">
          <a-input v-model="form.webhook_secret" placeholder="HMAC 签名密钥" />
        </a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive } from 'vue'
import { subscriptionApi } from '../../api/subscription'
import type { NotificationSubscription } from '../../types/notification'
import { Notification } from '@arco-design/web-vue'

const subs = ref<NotificationSubscription[]>([])
const loading = ref(false)
const modalVisible = ref(false)
const editing = ref(false)
const saving = ref(false)
const selectedChannels = ref<string[]>(['InApp'])
const editId = ref('')

const form = reactive({
  scope: 'global' as string,
  resource_id: '',
  event_types: [] as string[],
  webhook_url: '',
  webhook_secret: '',
})

function eventLabel(type: string): string {
  const map: Record<string, string> = {
    'workflow.started': '工作流启动', 'workflow.completed': '已完成',
    'workflow.failed': '执行失败', 'workflow.canceled': '已取消',
    'node.success': '节点成功', 'node.failed': '节点失败', 'node.skipped': '已跳过',
  }
  return map[type] || type
}

async function fetchSubs() {
  loading.value = true
  try {
    const res = await subscriptionApi.list()
    subs.value = res.data || []
  } catch {} finally { loading.value = false }
}

fetchSubs()

function openCreate() {
  editing.value = false
  editId.value = ''
  form.scope = 'global'
  form.resource_id = ''
  form.event_types = []
  selectedChannels.value = ['InApp']
  form.webhook_url = ''
  form.webhook_secret = ''
  modalVisible.value = true
}

function openEdit(s: NotificationSubscription) {
  editing.value = true
  editId.value = s.subscription_id
  form.scope = s.scope === 'Global' ? 'global' : 'resource'
  form.resource_id = s.resource_id || ''
  form.event_types = [...s.event_types]
  selectedChannels.value = s.channels.map(c => c.type)
  const wh = s.channels.find(c => c.type === 'Webhook')
  form.webhook_url = wh?.url || ''
  form.webhook_secret = wh?.secret || ''
  modalVisible.value = true
}

async function handleSave() {
  saving.value = true
  try {
    const channels = selectedChannels.value.map(c => {
      if (c === 'Webhook') return { type: 'Webhook', url: form.webhook_url, secret: form.webhook_secret || null }
      return { type: 'InApp' }
    })
    const scope = form.scope === 'global' ? 'Global' : 'Resource'
    if (editing.value) {
      await subscriptionApi.update(editId.value, {
        event_types: form.event_types,
        channels,
      })
      Notification.success({ content: '订阅已更新' })
    } else {
      await subscriptionApi.create({
        scope,
        resource_type: form.scope === 'resource' ? 'workflow_meta' : undefined,
        resource_id: form.scope === 'resource' ? form.resource_id : undefined,
        event_types: form.event_types,
        channels,
      })
      Notification.success({ content: '订阅已创建' })
    }
    modalVisible.value = false
    fetchSubs()
  } catch {} finally { saving.value = false }
}

async function toggleEnabled(s: NotificationSubscription, v: boolean) {
  try {
    await subscriptionApi.update(s.subscription_id, {
      event_types: s.event_types,
      channels: s.channels,
      enabled: v,
    })
    s.enabled = v
  } catch {}
}

async function handleDelete(id: string) {
  try {
    await subscriptionApi.delete(id)
    subs.value = subs.value.filter(s => s.subscription_id !== id)
    Notification.success({ content: '已删除' })
  } catch {}
}
</script>

<style scoped>
.subscription-page {
  max-width: 800px;
  margin: 0 auto;
}
.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}
</style>
