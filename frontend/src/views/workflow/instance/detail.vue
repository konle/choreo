<template>
  <div class="instance-detail-page">
    <a-page-header :title="`工作流实例: ${instance?.workflow_instance_id || ''}`" @back="$router.push('/workflows/instances')">
      <template #extra>
        <a-space>
          <a-button v-if="instance?.status === 'Pending' && canExecute" type="primary" @click="handleExecute">执行</a-button>
          <a-button v-if="canRetryNode && canExecute" @click="handleRetry">重试</a-button>
          <a-button v-if="instance?.status === 'Suspended' && canExecute" @click="handleResume">恢复</a-button>
          <a-button v-if="['Failed','Suspended'].includes(instance?.status || '') && canExecute" status="danger" @click="handleCancel">取消</a-button>
          <a-button v-if="canSkipNode" @click="openSkipModal">跳过当前节点</a-button>
        </a-space>
      </template>
    </a-page-header>

    <a-modal v-model:visible="skipModalVisible" title="跳过节点" @ok="submitSkip" @cancel="closeSkipModal">
      <p class="skip-hint">须为当前失败/挂起节点填写 <code>output</code> JSON 对象（可为空对象 <code>{}</code>），供下游 <code>nodes.&lt;node_id&gt;.output</code> 引用。提交后将投递编排回调，一般无需再点「执行」。</p>
      <a-textarea v-model="skipOutputText" :auto-size="{ minRows: 6, maxRows: 16 }" placeholder="{}" />
    </a-modal>

    <a-modal v-model:visible="childSkipModalVisible" title="跳过容器子任务" @ok="submitChildSkip" @cancel="childSkipModalVisible = false">
      <p class="skip-hint">为容器内失败的子任务填写 <code>output</code> JSON 对象（可为空对象 <code>{}</code>）。</p>
      <a-descriptions :column="1" size="small" style="margin-bottom: 12px">
        <a-descriptions-item label="子任务ID">{{ childSkipTarget }}</a-descriptions-item>
      </a-descriptions>
      <a-textarea v-model="childSkipOutputText" :auto-size="{ minRows: 6, maxRows: 16 }" placeholder="{}" />
    </a-modal>

    <a-row :gutter="16" align="stretch" class="instance-detail-row">
      <a-col :span="6">
        <a-card title="基本信息" :loading="loading" size="small" style="height: 100%">
          <a-descriptions :column="1" size="small">
            <a-descriptions-item label="状态">
              <status-tag v-if="instance" :status="instance.status" :map="WORKFLOW_INSTANCE_STATUS_MAP" />
            </a-descriptions-item>
            <a-descriptions-item label="版本">{{ instance?.workflow_version }}</a-descriptions-item>
            <a-descriptions-item label="当前节点">{{ instance?.current_node || '-' }}</a-descriptions-item>
            <a-descriptions-item label="创建时间">{{ formatDate(instance?.created_at || '') }}</a-descriptions-item>
          </a-descriptions>
          <a-divider>Context</a-divider>
          <json-viewer :data="instance?.context" />
        </a-card>
      </a-col>

      <a-col :span="12">
        <a-card title="节点拓扑" size="small" class="topology-card">
          <div ref="flowContainer" class="topology-flow-container">
            <VueFlow
              v-if="flowNodes.length"
              :nodes="flowNodes"
              :edges="flowEdges"
              :nodes-draggable="false"
              :nodes-connectable="false"
              fit-view-on-init
              @node-click="({ node }: any) => handleNodeClick(node.id)"
            >
              <Background />
            </VueFlow>
          </div>
        </a-card>
      </a-col>

      <a-col :span="6">
        <a-card title="节点详情" size="small" style="height: 100%">
          <template v-if="selectedNode">
            <a-descriptions :column="1" size="small">
              <a-descriptions-item label="节点ID">{{ selectedNode.node_id }}</a-descriptions-item>
              <a-descriptions-item label="类型">{{ selectedNode.node_type }}</a-descriptions-item>
              <a-descriptions-item label="状态">
                <status-tag :status="selectedNode.status" :map="NODE_STATUS_MAP" />
              </a-descriptions-item>
              <a-descriptions-item v-if="selectedNode.task_instance?.task_id" label="任务模板ID">
                <a-typography-text copyable>{{ selectedNode.task_instance.task_id }}</a-typography-text>
              </a-descriptions-item>
            </a-descriptions>
            <template v-if="isContainerNode(selectedNode)">
              <a-divider>子任务状态</a-divider>
              <a-table :data="containerChildTasks" :pagination="false" size="mini" :bordered="{ cell: true }">
                <template #columns>
                  <a-table-column title="子任务ID" data-index="id" :width="180" ellipsis />
                  <a-table-column title="状态" data-index="status" :width="80">
                    <template #cell="{ record }">
                      <a-tag :color="childStatusColor(record.status)">{{ record.status }}</a-tag>
                    </template>
                  </a-table-column>
                  <a-table-column title="操作" :width="120">
                    <template #cell="{ record }">
                      <a-space v-if="record.status === 'Failed' && canExecute">
                        <a-button type="text" size="mini" @click="handleChildRetry(record.id)">重试</a-button>
                        <a-button type="text" size="mini" @click="openChildSkipModal(record.id)">跳过</a-button>
                      </a-space>
                    </template>
                  </a-table-column>
                </template>
              </a-table>
            </template>

            <template v-if="selectedNode.node_type === 'SubWorkflow' && subWorkflowInstanceId">
              <a-divider>子工作流实例</a-divider>
              <router-link :to="`/workflows/instances/${subWorkflowInstanceId}`">
                <a-link>{{ subWorkflowInstanceId }}</a-link>
              </router-link>
            </template>

            <template v-if="selectedNode.node_type === 'Llm' && selectedNode.task_instance?.output">
              <a-divider>LLM 结果</a-divider>
              <a-descriptions :column="1" size="small">
                <a-descriptions-item label="模型">
                  {{ selectedNode.task_instance.output.model || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="Finish Reason">
                  {{ selectedNode.task_instance.output.finish_reason || '-' }}
                </a-descriptions-item>
                <a-descriptions-item v-if="selectedNode.task_instance.output.usage" label="Token 用量">
                  prompt={{ selectedNode.task_instance.output.usage.prompt_tokens }},
                  completion={{ selectedNode.task_instance.output.usage.completion_tokens }},
                  total={{ selectedNode.task_instance.output.usage.total_tokens }}
                </a-descriptions-item>
              </a-descriptions>
              <a-form-item label="Content" style="margin-top: 8px">
                <a-textarea
                  :model-value="typeof selectedNode.task_instance.output.content === 'string' ? selectedNode.task_instance.output.content : JSON.stringify(selectedNode.task_instance.output.content, null, 2)"
                  :auto-size="{ minRows: 2, maxRows: 15 }"
                  readonly
                />
              </a-form-item>
              <template v-if="selectedNode.task_instance.output.parsed">
                <a-form-item label="Parsed (JSON)">
                  <json-viewer :data="selectedNode.task_instance.output.parsed" />
                </a-form-item>
              </template>
            </template>

            <template v-if="selectedNode.node_type === 'Pause' && selectedNode.status === 'Suspended'">
              <a-divider>暂停节点</a-divider>
              <a-descriptions :column="1" size="small">
                <a-descriptions-item label="模式">
                  {{ pauseNodeMode === 'Auto' ? '自动' : '手动' }}
                </a-descriptions-item>
                <a-descriptions-item label="恢复时间">
                  {{ pauseResumeAt ? formatDate(pauseResumeAt) : '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="状态">
                  <a-tag v-if="pauseExpired" color="green">计时已到期</a-tag>
                  <a-tag v-else color="orange">等待中（{{ pauseCountdown }}s）</a-tag>
                </a-descriptions-item>
              </a-descriptions>
              <a-button
                v-if="pauseNodeMode === 'Manual' && pauseExpired && canExecute"
                type="primary"
                style="margin-top: 8px"
                :loading="resumingNode"
                @click="handleResumeNode"
              >
                确认继续
              </a-button>
            </template>

            <a-divider>节点上下文</a-divider>
            <p class="node-context-hint">
              执行本节点前用于模板 / Rhai 解析的合并上下文（含变量合并与系统注入的 <code>nodes</code>）。
            </p>
            <json-viewer :data="selectedNode.context ?? {}" />
            <a-divider>Input</a-divider>
            <json-viewer v-if="selectedNode.task_instance?.input != null" :data="selectedNode.task_instance.input" />
            <a-empty v-else description="暂无执行入参" />
            <a-divider>Output</a-divider>
            <json-viewer v-if="selectedNode.task_instance?.output != null" :data="selectedNode.task_instance.output" />
            <a-empty v-else description="暂无输出" />
            <template v-if="selectedNode.error_message">
              <a-divider>Error</a-divider>
              <a-alert type="error" :title="selectedNode.error_message" />
            </template>
          </template>
          <a-empty v-else description="点击节点查看详情" />
        </a-card>
      </a-col>
    </a-row>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { VueFlow } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { workflowApi } from '../../../api/workflow'
import { usePermission } from '../../../composables/use-permission'
import { usePolling } from '../../../composables/use-polling'
import { WORKFLOW_INSTANCE_STATUS_MAP, NODE_STATUS_MAP } from '../../../utils/constants'
import { formatDate } from '../../../utils/format'
import StatusTag from '../../../components/common/status-tag.vue'
import JsonViewer from '../../../components/common/json-viewer.vue'
import { Notification } from '@arco-design/web-vue'
import type { WorkflowInstanceEntity, WorkflowNodeInstanceEntity } from '../../../types/workflow'
import dagre from 'dagre'

const route = useRoute()
const { canExecute } = usePermission()
const instanceId = route.params.id as string

const instance = ref<WorkflowInstanceEntity | null>(null)
const loading = ref(false)
const selectedNode = ref<WorkflowNodeInstanceEntity | null>(null)
const skipModalVisible = ref(false)
const skipOutputText = ref('{}')
const childSkipModalVisible = ref(false)
const childSkipOutputText = ref('{}')
const childSkipTarget = ref('')

const CONTAINER_TYPES = new Set(['Parallel', 'ForkJoin'])
const UNSKIPPABLE_TYPES = new Set(['Parallel', 'ForkJoin', 'SubWorkflow'])

const canSkipNode = computed(() => {
  if (!canExecute.value || !instance.value) return false
  const st = instance.value.status
  if (st !== 'Failed' && st !== 'Suspended') return false
  const cur = instance.value.current_node
  const n = instance.value.nodes.find(x => x.node_id === cur)
  if (!n) return false
  if (n.status !== 'Failed' && n.status !== 'Suspended') return false
  if (UNSKIPPABLE_TYPES.has(n.node_type)) return false
  return true
})

const canRetryNode = computed(() => {
  if (!canExecute.value || !instance.value) return false
  if (instance.value.status !== 'Failed') return false
  const cur = instance.value.current_node
  const n = instance.value.nodes.find(x => x.node_id === cur)
  if (!n || n.status !== 'Failed') return false
  if (CONTAINER_TYPES.has(n.node_type)) return false
  return true
})

const NODE_COLORS: Record<string, string> = {
  Pending: '#C9CDD4',
  Running: '#3491FA',
  Success: '#00B42A',
  Failed: '#F53F3F',
  Suspended: '#F77234',
  Skipped: '#E5E6EB',
}

function collectEdges(nodes: WorkflowNodeInstanceEntity[]) {
  const edges: { source: string; target: string; label?: string; branch?: boolean }[] = []
  for (const n of nodes) {
    if (n.next_node) {
      edges.push({ source: n.node_id, target: n.next_node })
    }
    if (n.node_type === 'IfCondition') {
      const tpl = (n.task_instance.task_template as { IfCondition: { then_task?: string | null; else_task?: string | null } }).IfCondition
      if (tpl?.then_task) edges.push({ source: n.node_id, target: tpl.then_task, label: 'Y', branch: true })
      if (tpl?.else_task) edges.push({ source: n.node_id, target: tpl.else_task, label: 'N', branch: true })
    }
  }
  const seen = new Set<string>()
  return edges.filter(e => {
    const key = `${e.source}->${e.target}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

const flowNodes = computed(() => {
  if (!instance.value) return []
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'TB', nodesep: 60, ranksep: 100 })
  g.setDefaultEdgeLabel(() => ({}))
  for (const n of instance.value.nodes) {
    g.setNode(n.node_id, { width: 150, height: 40 })
  }
  for (const e of collectEdges(instance.value.nodes)) {
    g.setEdge(e.source, e.target)
  }
  dagre.layout(g)
  return instance.value.nodes.map(n => {
    const pos = g.node(n.node_id)
    return {
      id: n.node_id,
      position: { x: pos?.x || 0, y: pos?.y || 0 },
      data: { label: `${n.node_id} (${n.node_type})` },
      style: {
        background: NODE_COLORS[n.status] || '#C9CDD4',
        color: ['Running', 'Failed', 'Success'].includes(n.status) ? '#fff' : '#1D2129',
        borderRadius: '6px',
        padding: '6px 12px',
        fontSize: '12px',
        border: 'none',
        cursor: 'pointer',
      },
    }
  })
})

const flowEdges = computed(() => {
  if (!instance.value) return []
  return collectEdges(instance.value.nodes).map(e => ({
    id: `${e.source}->${e.target}`,
    source: e.source,
    target: e.target,
    animated: instance.value!.nodes.find(n => n.node_id === e.source)?.status === 'Running',
    label: e.label,
    style: e.branch ? { strokeDasharray: '5 5' } : {},
  }))
})

async function fetchInstance() {
  try {
    const res = await workflowApi.getInstance(instanceId)
    instance.value = res.data
  } catch {}
}

const { start: startPolling, stop: stopPolling } = usePolling(fetchInstance, 5000)

watch(() => instance.value?.status, (status) => {
  if (status && ['Completed', 'Canceled'].includes(status)) {
    stopPolling()
  }
})

watch(() => instance.value, () => {
  if (selectedNode.value && instance.value) {
    selectedNode.value = instance.value.nodes.find(n => n.node_id === selectedNode.value!.node_id) || null
  }
})

function handleNodeClick(nodeId: string) {
  selectedNode.value = instance.value?.nodes.find(n => n.node_id === nodeId) || null
}

async function handleExecute() {
  await workflowApi.executeInstance(instanceId)
  Notification.success({ content: '已执行' })
  fetchInstance()
}

async function handleRetry() {
  if (!instance.value) return
  await workflowApi.retryNode(instanceId, { node_id: instance.value.current_node })
  Notification.success({ content: '已重试' })
  fetchInstance()
}

async function handleResume() {
  await workflowApi.resumeInstance(instanceId)
  Notification.success({ content: '已恢复' })
  fetchInstance()
}

async function handleCancel() {
  await workflowApi.cancelInstance(instanceId)
  Notification.success({ content: '已取消' })
  fetchInstance()
}


function openSkipModal() {
  skipOutputText.value = '{}'
  skipModalVisible.value = true
}

function closeSkipModal() {
  skipModalVisible.value = false
}

async function submitSkip() {
  if (!instance.value) return
  let output: Record<string, unknown>
  try {
    const parsed = JSON.parse(skipOutputText.value || '{}')
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      Notification.error({ content: 'output 须为 JSON 对象' })
      return
    }
    output = parsed as Record<string, unknown>
  } catch {
    Notification.error({ content: 'output 不是合法 JSON' })
    return
  }
  const nodeId = instance.value.current_node
  try {
    await workflowApi.skipNode(instanceId, { node_id: nodeId, output })
    Notification.success({ content: '已跳过并投递编排' })
    skipModalVisible.value = false
    fetchInstance()
  } catch {
    /* axios 拦截器已提示 */
  }
}


function isContainerNode(node: WorkflowNodeInstanceEntity | null): boolean {
  return !!node && CONTAINER_TYPES.has(node.node_type)
}

interface ChildTaskInfo { id: string; status: string }

const containerChildTasks = computed<ChildTaskInfo[]>(() => {
  const node = selectedNode.value
  if (!node || !CONTAINER_TYPES.has(node.node_type)) return []
  const output = node.task_instance?.output
  if (!output || typeof output !== 'object') return []

  const total = (output as any).total_items ?? (output as any).total_tasks ?? 0
  const results: Record<string, any> | undefined = (output as any).results
  const instanceId = instance.value?.workflow_instance_id || ''
  const dispatched = (output as any).dispatched_count || 0

  const items: ChildTaskInfo[] = []
  const isForkJoin = node.node_type === 'ForkJoin'

  if (isForkJoin && results) {
    for (const [key, entry] of Object.entries(results)) {
      const id = key
      let status = 'Pending'
      if (entry && typeof entry === 'object' && entry.status) {
        status = entry.status
      }
      items.push({ id, status })
    }
  } else {
    for (let i = 0; i < total; i++) {
      const id = `${instanceId}-${node.node_id}-${i}`
      let status = 'Pending'
      if (results && results[id] && typeof results[id] === 'object' && results[id].status) {
        status = results[id].status
      } else if (i < dispatched) {
        status = 'Running'
      }
      items.push({ id, status })
    }
  }
  return items
})

const subWorkflowInstanceId = computed<string | null>(() => {
  const node = selectedNode.value
  if (!node || node.node_type !== 'SubWorkflow') return null
  const output = node.task_instance?.output
  if (!output || typeof output !== 'object') return null
  return (output as any).child_workflow_instance_id || null
})

const pauseNodeMode = computed(() => {
  const output = selectedNode.value?.task_instance?.output as any
  return output?.mode || 'Auto'
})

const pauseResumeAt = computed(() => {
  const output = selectedNode.value?.task_instance?.output as any
  return output?.resume_at || null
})

const pauseCountdown = ref(0)
const pauseExpired = computed(() => pauseCountdown.value <= 0 && !!pauseResumeAt.value)

let pauseTimer: ReturnType<typeof setInterval> | null = null

function updatePauseCountdown() {
  if (!pauseResumeAt.value) {
    pauseCountdown.value = 0
    return
  }
  pauseCountdown.value = Math.max(0, Math.ceil((new Date(pauseResumeAt.value).getTime() - Date.now()) / 1000))
}

watch(pauseResumeAt, (val) => {
  if (pauseTimer) { clearInterval(pauseTimer); pauseTimer = null }
  if (val) {
    updatePauseCountdown()
    pauseTimer = setInterval(updatePauseCountdown, 1000)
  }
}, { immediate: true })

onUnmounted(() => { if (pauseTimer) clearInterval(pauseTimer) })

const resumingNode = ref(false)

async function handleResumeNode() {
  if (!instance.value || !selectedNode.value) return
  resumingNode.value = true
  try {
    await workflowApi.resumeNode(instanceId, { node_id: selectedNode.value.node_id })
    Notification.success({ content: '已确认继续' })
    fetchInstance()
  } catch (e: any) {
    Notification.error({ content: e?.message || '操作失败' })
  } finally {
    resumingNode.value = false
  }
}

function childStatusColor(status: string): string {
  switch (status) {
    case 'Success': return 'green'
    case 'Failed': return 'red'
    case 'Skipped': return 'gray'
    case 'Running': return 'blue'
    default: return 'gray'
  }
}

async function handleChildRetry(childTaskId: string) {
  if (!instance.value || !selectedNode.value) return
  try {
    await workflowApi.retryNode(instanceId, {
      node_id: selectedNode.value.node_id,
      child_task_id: childTaskId,
    })
    Notification.success({ content: '已重试子任务' })
    fetchInstance()
  } catch (e: any) {
    Notification.error({ content: e?.message || '重试失败' })
  }
}

function openChildSkipModal(childTaskId: string) {
  childSkipTarget.value = childTaskId
  childSkipOutputText.value = '{}'
  childSkipModalVisible.value = true
}

async function submitChildSkip() {
  if (!instance.value || !selectedNode.value) return
  let output: Record<string, unknown>
  try {
    const parsed = JSON.parse(childSkipOutputText.value || '{}')
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      Notification.error({ content: 'output 须为 JSON 对象' })
      return
    }
    output = parsed as Record<string, unknown>
  } catch {
    Notification.error({ content: 'output 不是合法 JSON' })
    return
  }
  try {
    await workflowApi.skipNode(instanceId, {
      node_id: selectedNode.value.node_id,
      child_task_id: childSkipTarget.value,
      output,
    })
    Notification.success({ content: '已跳过子任务并投递编排' })
    childSkipModalVisible.value = false
    fetchInstance()
  } catch {
    /* axios 拦截器已提示 */
  }
}

onMounted(async () => {
  loading.value = true
  await fetchInstance()
  loading.value = false
  if (instance.value && !['Completed', 'Canceled'].includes(instance.value.status)) {
    startPolling()
  }
})
</script>

<style scoped>
.instance-detail-page {
  display: flex;
  flex-direction: column;
  min-height: calc(100vh - 128px);
}
.instance-detail-row {
  flex: 1;
}

.topology-card {
  min-height: 500px;
  height: 100%;
  display: flex;
  flex-direction: column;
}
.topology-card :deep(.arco-card-body) {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
}
.topology-flow-container {
  width: 100%;
  flex: 1;
  min-height: 420px;
}

.node-context-hint {
  margin: 0 0 8px;
  font-size: 12px;
  color: var(--color-text-3);
  line-height: 1.5;
}
.node-context-hint code {
  font-size: 11px;
  padding: 0 4px;
  border-radius: 2px;
  background: var(--color-fill-2);
}

.skip-hint {
  margin: 0 0 12px;
  font-size: 12px;
  color: var(--color-text-3);
  line-height: 1.5;
}
.skip-hint code {
  font-size: 11px;
  padding: 0 4px;
  border-radius: 2px;
  background: var(--color-fill-2);
}
</style>

<style>
@import '@vue-flow/core/dist/style.css';
@import '@vue-flow/core/dist/theme-default.css';
</style>
