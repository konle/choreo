<template>
  <div class="editor-container">
    <div class="editor-header">
      <a-button @click="$router.back()">← 返回</a-button>
      <span class="editor-title">{{ metaName }} <a-tag v-if="currentVersion">v{{ currentVersion }}</a-tag>
        <a-tag v-if="versionStatus" :color="versionStatus === 'Draft' ? 'gray' : versionStatus === 'Published' ? 'green' : 'orange'">{{ versionStatus }}</a-tag>
      </span>
      <a-space>
        <a-button v-if="!readonly" type="primary" @click="handleSave" :loading="saving">保存</a-button>
        <a-tag v-if="readonly" color="orange">只读模式（已发布版本不可编辑）</a-tag>
      </a-space>
    </div>

    <div class="editor-body">
      <div class="editor-panel-left">
        <div class="panel-title">节点面板</div>
        <div
          v-for="nt in nodeTypes"
          :key="nt.type"
          class="node-item"
          :style="{ borderLeftColor: nt.color, opacity: readonly ? 0.5 : 1, cursor: readonly ? 'not-allowed' : 'grab' }"
          :draggable="!readonly"
          @dragstart="onDragStart($event, nt)"
        >
          <span class="node-dot" :style="{ background: nt.color }"></span>
          {{ nt.label }}
        </div>
      </div>

      <div class="editor-canvas" @drop="onDrop" @dragover.prevent>
        <VueFlow
          v-model:nodes="nodes"
          v-model:edges="edges"
          :node-types="customNodeTypes as any"
          :connect-on-click="false"
          @node-click="onNodeClick"
          @connect="onConnect"
          @edge-click="onEdgeClick"
          @pane-click="onPaneClick"
          @keydown="onKeyDown"
          fit-view-on-init
          :default-edge-options="{ type: 'smoothstep', animated: false }"
        >
          <Background />
          <Controls />
          <MiniMap />
          <Panel :position="PanelPosition.BottomLeft" class="shortcut-hint">
            Ctrl+Z 撤回 | Ctrl+Shift+Z 重做
          </Panel>
        </VueFlow>
      </div>

      <div class="editor-panel-right">
        <div class="panel-title">属性面板</div>
        <template v-if="selectedNode">
          <a-form :model="selectedNode.data" layout="vertical" size="small">
            <a-form-item label="节点ID">
              <a-input :model-value="selectedNode.id" disabled />
            </a-form-item>
            <a-form-item label="节点类型">
              <a-input :model-value="selectedNode.data.nodeType" disabled />
            </a-form-item>

            <!-- ===== 引用原子任务: Http / Approval / gRPC ===== -->
            <template v-if="isTaskRefNode(selectedNode.data.nodeType)">
              <PublishedTaskRefFields
                v-model:task-id="selectedNode.data.taskId"
                v-model:form-fields="selectedNode.data.formFields"
                :task-type="selectedNode.data.nodeType"
                :tasks="getTasksForType(selectedNode.data.nodeType)"
                :task-snapshot="selectedNode.data.taskSnapshot"
                :readonly="readonly"
                @change="onTaskSelected(selectedNode)"
                @update:form-fields="pushSnapshot"
              />
            </template>

            <!-- ===== 引用工作流: SubWorkflow ===== -->
            <template v-else-if="selectedNode.data.nodeType === 'SubWorkflow'">
              <SubworkflowRefFields
                v-model:meta-id="selectedNode.data.subWorkflowMetaId"
                v-model:version="selectedNode.data.subWorkflowVersion"
                v-model:timeout="selectedNode.data.config.timeout"
                v-model:form-fields="selectedNode.data.formFields"
                :sub-workflow-meta="selectedNode.data.subWorkflowMeta"
                :sub-workflow-versions="selectedNode.data.subWorkflowVersions"
                :workflow-metas="workflowMetas"
                :readonly="readonly"
                @meta-change="onSubWorkflowMetaSelected(selectedNode)"
                @version-change="onSubWorkflowVersionSelected(selectedNode)"
                @update:timeout="pushSnapshot"
                @update:form-fields="pushSnapshot"
              />
            </template>

            <!-- ===== IfCondition ===== -->
            <template v-else-if="selectedNode.data.nodeType === 'IfCondition'">
              <a-form-item label="条件名称">
                <a-input v-model="selectedNode.data.config.name" :disabled="readonly" @change="updateNodeLabel(selectedNode)" />
              </a-form-item>
              <a-form-item label="条件表达式 (Rhai)">
                <a-textarea v-model="selectedNode.data.config.condition" :auto-size="{ minRows: 3 }" :disabled="readonly" />
              </a-form-item>
              <a-divider>分支连接</a-divider>
              <a-form-item label="Then → 节点">
                <a-input :model-value="getThenTarget(selectedNode.id)" disabled placeholder="从 True 端口拖线到目标节点" />
              </a-form-item>
              <a-form-item label="Else → 节点">
                <a-input :model-value="getElseTarget(selectedNode.id)" disabled placeholder="从 False 端口拖线到目标节点" />
              </a-form-item>
            </template>

            <!-- ===== ContextRewrite ===== -->
            <template v-else-if="selectedNode.data.nodeType === 'ContextRewrite'">
              <a-form-item label="名称">
                <a-input v-model="selectedNode.data.config.name" :disabled="readonly" @change="updateNodeLabel(selectedNode)" />
              </a-form-item>
              <a-form-item label="Rhai 脚本">
                <a-textarea v-model="selectedNode.data.config.script" :auto-size="{ minRows: 4 }" :disabled="readonly" />
              </a-form-item>
              <a-form-item label="合并模式">
                <a-select v-model="selectedNode.data.config.merge_mode" :disabled="readonly">
                  <a-option value="Merge">Merge</a-option>
                  <a-option value="Replace">Replace</a-option>
                </a-select>
              </a-form-item>
            </template>

            <!-- ===== Parallel ===== -->
            <template v-else-if="selectedNode.data.nodeType === 'Parallel'">
              <a-form-item label="数据路径 (items_path)">
                <a-input v-model="selectedNode.data.config.items_path" :disabled="readonly" @change="pushSnapshot" />
              </a-form-item>
              <a-form-item label="迭代变量名 (item_alias)">
                <a-input v-model="selectedNode.data.config.item_alias" :disabled="readonly" @change="pushSnapshot" />
              </a-form-item>
              <a-form-item label="并发度">
                <a-input-number v-model="selectedNode.data.config.concurrency" :min="1" :disabled="readonly" @change="pushSnapshot" />
              </a-form-item>
              <a-form-item label="模式">
                <a-select v-model="selectedNode.data.config.mode" :disabled="readonly" @change="pushSnapshot">
                  <a-option value="Rolling">Rolling</a-option>
                  <a-option value="Batch">Batch</a-option>
                </a-select>
              </a-form-item>
              <a-form-item label="最大失败数">
                <a-input-number v-model="selectedNode.data.config.max_failures" :min="0" :disabled="readonly" @change="pushSnapshot" />
              </a-form-item>
              <ParallelInnerTaskPanel
                :node-data="selectedNode.data"
                :task-cache="taskCache"
                :workflow-metas="workflowMetas"
                :readonly="readonly"
                @change="onParallelInnerPanelChange"
              />
            </template>

            <!-- ===== ForkJoin ===== -->
            <template v-else-if="selectedNode.data.nodeType === 'ForkJoin'">
              <a-form-item label="并发度">
                <a-input-number v-model="selectedNode.data.config.concurrency" :min="1" :disabled="readonly" />
              </a-form-item>
              <a-form-item label="模式">
                <a-select v-model="selectedNode.data.config.mode" :disabled="readonly">
                  <a-option value="Rolling">Rolling</a-option>
                  <a-option value="Batch">Batch</a-option>
                </a-select>
              </a-form-item>
              <a-form-item label="子任务 (JSON)">
                <a-textarea v-model="selectedNode.data.config.tasksJson" :auto-size="{ minRows: 4 }" :disabled="readonly" />
              </a-form-item>
            </template>

            <!-- ===== Pause ===== -->
            <template v-else-if="selectedNode.data.nodeType === 'Pause'">
              <a-form-item label="等待时间（秒）">
                <a-input-number v-model="selectedNode.data.config.wait_seconds" :min="1" :disabled="readonly" />
              </a-form-item>
              <a-form-item label="模式">
                <a-select v-model="selectedNode.data.config.mode" :disabled="readonly">
                  <a-option value="Auto">自动（到期后自动完成）</a-option>
                  <a-option value="Manual">手动（到期后需人工确认）</a-option>
                </a-select>
              </a-form-item>
            </template>

            <a-form-item label="Context (JSON)">
              <a-textarea v-model="selectedNode.data.contextJson" :auto-size="{ minRows: 2 }" :disabled="readonly" />
            </a-form-item>

            <a-button v-if="!readonly" status="danger" long @click="handleDeleteNode">删除节点</a-button>
          </a-form>
        </template>

        <template v-else-if="selectedEdge">
          <a-form layout="vertical" size="small">
            <a-form-item label="连线">
              <a-input :model-value="`${selectedEdge.source} → ${selectedEdge.target}`" disabled />
            </a-form-item>
            <a-form-item v-if="selectedEdge.sourceHandle" label="分支">
              <a-tag :color="selectedEdge.sourceHandle === 'then' ? 'green' : 'red'">
                {{ selectedEdge.sourceHandle === 'then' ? 'True' : 'False' }}
              </a-tag>
            </a-form-item>
            <a-button v-if="!readonly" status="danger" long @click="deleteSelectedEdge">删除连线</a-button>
          </a-form>
        </template>

        <a-empty v-else description="点击节点编辑属性，拖拽端口连线" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, markRaw, computed } from 'vue'
import { useRoute } from 'vue-router'
import { VueFlow, useVueFlow, Panel, PanelPosition, type Node, type Edge, type Connection } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'
import { MiniMap } from '@vue-flow/minimap'
import { workflowApi } from '../../../api/workflow'
import { taskApi } from '../../../api/task'
import { Notification, Modal } from '@arco-design/web-vue'
import type { TaskEntity } from '../../../types/task'
import type { WorkflowMetaEntity } from '../../../types/workflow'
import dagre from 'dagre'
import WorkflowNode from './workflow-node.vue'
import ConditionNode from './condition-node.vue'
import PublishedTaskRefFields from './published-task-ref-fields.vue'
import SubworkflowRefFields from './subworkflow-ref-fields.vue'
import ParallelInnerTaskPanel from './parallel-inner-task-panel.vue'
import { buildFormFields, formFieldsToFormArray, type EditorFormField } from './workflow-editor-form-utils'
import {
  buildParallelTaskTemplateForSave,
  defaultParallelInnerTemplate,
  detectParallelInnerKind,
  hydrateParallelEditorState,
} from './parallel-inner-task-utils'

const customNodeTypes = {
  workflow: markRaw(WorkflowNode),
  condition: markRaw(ConditionNode),
}

const route = useRoute()
const metaId = route.params.metaId as string
const versionParam = route.params.version ? Number(route.params.version) : null

const metaName = ref('')
const currentVersion = ref(versionParam || 0)
const versionStatus = ref<string>('')
const saving = ref(false)
const readonly = computed(() => versionStatus.value !== '' && versionStatus.value !== 'Draft')

const nodes = ref<Node[]>([])
const edges = ref<Edge[]>([])
const selectedNode = ref<Node | null>(null)
const selectedEdge = ref<Edge | null>(null)

const taskCache = ref<TaskEntity[]>([])
const workflowMetas = ref<WorkflowMetaEntity[]>([])

let nodeCounter = 0

const TASK_REF_TYPES = ['Http', 'Approval', 'Grpc', 'Llm']
function isTaskRefNode(nodeType: string) { return TASK_REF_TYPES.includes(nodeType) }
function getTasksForType(nodeType: string): TaskEntity[] {
  return taskCache.value.filter(t => t.task_type === nodeType && t.status === 'Published')
}

const nodeTypes = [
  { type: 'Http', label: 'HTTP', color: '#3491FA' },
  { type: 'IfCondition', label: '条件分支', color: '#F7BA1E' },
  { type: 'ContextRewrite', label: '上下文重写', color: '#14C9C9' },
  { type: 'Parallel', label: '并发容器', color: '#00B42A' },
  { type: 'ForkJoin', label: '异构并发', color: '#009A29' },
  { type: 'SubWorkflow', label: '子工作流', color: '#86909C' },
  { type: 'Grpc', label: 'gRPC', color: '#722ED1' },
  { type: 'Approval', label: '审批', color: '#F77234' },
  { type: 'Pause', label: '暂停', color: '#F5A623' },
  { type: 'Llm', label: 'LLM 大模型', color: '#7B61FF' },
]

function parallelNodeDataDefaults() {
  return {
    parallelInnerKind: 'Http',
    parallelInnerTaskId: null as string | null,
    parallelInnerSnapshot: null as Record<string, unknown> | string | null,
    parallelInnerFormFields: [] as EditorFormField[],
    parallelSubWorkflowMetaId: null as string | null,
    parallelSubWorkflowVersion: null as number | null,
    parallelSubWorkflowMeta: null as WorkflowMetaEntity | null,
    parallelSubWorkflowVersions: [] as { version: number; nodes?: unknown[] }[],
    parallelSubWorkflowFormFields: [] as EditorFormField[],
    parallelSubWorkflowTimeout: null as number | null,
  }
}

function getDefaultConfig(type: string): any {
  switch (type) {
    case 'Http': case 'Approval': case 'Grpc': return {}
    case 'Pause': return { wait_seconds: 10, mode: 'Auto' }
    case 'Llm': return {}
    case 'SubWorkflow': return { timeout: null }
    case 'IfCondition': return { name: '', condition: '' }
    case 'ContextRewrite': return { name: '', script: '', merge_mode: 'Merge' }
    case 'Parallel':
      return {
        items_path: '',
        item_alias: 'item',
        concurrency: 10,
        mode: 'Rolling',
        max_failures: null,
        task_template: defaultParallelInnerTemplate('Http'),
      }
    case 'ForkJoin': return { concurrency: 5, mode: 'Rolling', max_failures: null, tasksJson: '[]' }
    default: return {}
  }
}

function getVueFlowNodeType(t: string) { return t === 'IfCondition' ? 'condition' : 'workflow' }

// ---- Node label ----

function resolveLabel(nodeId: string, nodeType: string, data: any): string {
  if (isTaskRefNode(nodeType) && data.taskId) {
    const task = taskCache.value.find(t => t.id === data.taskId)
    if (task) return task.name
  }
  if (nodeType === 'SubWorkflow' && data.subWorkflowMeta) {
    const ver = data.subWorkflowVersion ? ` v${data.subWorkflowVersion}` : ''
    return `${data.subWorkflowMeta.name}${ver}`
  }
  if (nodeType === 'Parallel') {
    const kind = data.parallelInnerKind || detectParallelInnerKind(data.config?.task_template)
    if ((kind === 'Http' || kind === 'Grpc') && data.parallelInnerTaskId) {
      const task = taskCache.value.find(t => t.id === data.parallelInnerTaskId)
      if (task) return `${nodeId} · ${task.name}`
    }
    if (kind === 'SubWorkflow' && data.parallelSubWorkflowMeta) {
      const ver = data.parallelSubWorkflowVersion != null ? ` v${data.parallelSubWorkflowVersion}` : ''
      return `${nodeId} · ${data.parallelSubWorkflowMeta.name}${ver}`
    }
  }
  if (['IfCondition', 'ContextRewrite'].includes(nodeType) && data.config?.name) {
    return data.config.name
  }
  return `${nodeId} (${nodeType})`
}

function updateNodeLabel(node: Node) {
  const d = node.data as Record<string, unknown>
  d.label = resolveLabel(node.id, String(d.nodeType), d)
}

// ---- IfCondition edge helpers ----

function getThenTarget(nodeId: string): string {
  for (const e of edges.value) {
    if (e.source === nodeId && e.sourceHandle === 'then') return e.target
  }
  return '(未连接)'
}
function getElseTarget(nodeId: string): string {
  for (const e of edges.value) {
    if (e.source === nodeId && e.sourceHandle === 'else') return e.target
  }
  return '(未连接)'
}

// ---- Task / SubWorkflow selection ----

function onTaskSelected(node: Node) {
  const task = taskCache.value.find(t => t.id === node.data.taskId)
  if (!task) { node.data.taskSnapshot = null; node.data.formFields = []; return }
  node.data.taskSnapshot = task.task_template
  const inner = (task.task_template as any)[node.data.nodeType]
  node.data.formFields = inner?.form ? buildFormFields(inner.form) : []
  updateNodeLabel(node)
  pushSnapshot()
}

async function onSubWorkflowMetaSelected(node: Node) {
  const id = node.data.subWorkflowMetaId
  if (!id) return
  const meta = workflowMetas.value.find(m => m.workflow_meta_id === id)
  node.data.subWorkflowMeta = meta || null
  try {
    const res = await workflowApi.listTemplates(id)
    node.data.subWorkflowVersions = res.data
    if (res.data.length > 0) node.data.subWorkflowVersion = res.data[res.data.length - 1].version
  } catch { node.data.subWorkflowVersions = [] }
  node.data.formFields = meta?.form?.length ? buildFormFields(meta.form) : []
  updateNodeLabel(node)
  pushSnapshot()
}
function onSubWorkflowVersionSelected(node: Node) { updateNodeLabel(node); pushSnapshot() }

function onParallelInnerPanelChange() {
  const node = selectedNode.value as Node | null
  if (!node) return
  const d = node.data as Record<string, unknown>
  d.label = resolveLabel(node.id, String(d.nodeType), d as any)
  pushSnapshot()
}

async function refreshParallelSubWorkflowVersions(data: Record<string, unknown>) {
  const id = data.parallelSubWorkflowMetaId as string | null
  if (!id) return
  try {
    const res = await workflowApi.listTemplates(id)
    data.parallelSubWorkflowVersions = res.data
  } catch {
    data.parallelSubWorkflowVersions = []
  }
}

// ---- Undo / Redo ----

interface Snapshot { nodes: string; edges: string }
const history: Snapshot[] = []
let historyIndex = -1
const MAX_HISTORY = 50

function takeSnapshot(): Snapshot {
  return { nodes: JSON.stringify(nodes.value), edges: JSON.stringify(edges.value) }
}
function pushSnapshot() {
  const snap = takeSnapshot()
  if (historyIndex < history.length - 1) history.splice(historyIndex + 1)
  history.push(snap)
  if (history.length > MAX_HISTORY) history.shift()
  historyIndex = history.length - 1
}
function restoreSnapshot(snap: Snapshot) {
  nodes.value = JSON.parse(snap.nodes)
  edges.value = JSON.parse(snap.edges)
  selectedNode.value = null
  selectedEdge.value = null
}
function undo() {
  if (historyIndex <= 0) return
  historyIndex--
  restoreSnapshot(history[historyIndex])
}
function redo() {
  if (historyIndex >= history.length - 1) return
  historyIndex++
  restoreSnapshot(history[historyIndex])
}

function handleGlobalKeyDown(e: KeyboardEvent) {
  if ((e.ctrlKey || e.metaKey) && e.key === 'z' && !e.shiftKey) { e.preventDefault(); undo() }
  if ((e.ctrlKey || e.metaKey) && (e.key === 'Z' || (e.key === 'z' && e.shiftKey) || e.key === 'y')) { e.preventDefault(); redo() }
}
onMounted(() => document.addEventListener('keydown', handleGlobalKeyDown))
onUnmounted(() => document.removeEventListener('keydown', handleGlobalKeyDown))

// ---- Connection handling ----

function onConnect(connection: Connection) {
  if (readonly.value) return
  const isCondHandle = connection.sourceHandle === 'then' || connection.sourceHandle === 'else'
  if (isCondHandle) {
    const idx = edges.value.findIndex(e => e.source === connection.source && e.sourceHandle === connection.sourceHandle)
    if (idx !== -1) edges.value.splice(idx, 1)
  } else {
    const idx = edges.value.findIndex(e => e.source === connection.source && !e.sourceHandle)
    if (idx !== -1) edges.value.splice(idx, 1)
  }
  const color = connection.sourceHandle === 'then' ? '#00B42A' : connection.sourceHandle === 'else' ? '#F53F3F' : undefined
  const label = connection.sourceHandle === 'then' ? 'True' : connection.sourceHandle === 'else' ? 'False' : undefined
  edges.value.push({
    id: `${connection.source}-${connection.sourceHandle || 'next'}->${connection.target}`,
    source: connection.source!, target: connection.target!,
    sourceHandle: connection.sourceHandle || undefined,
    targetHandle: connection.targetHandle || undefined,
    type: 'smoothstep',
    style: color ? { stroke: color, strokeWidth: 2 } : undefined,
    label, labelStyle: color ? { fill: color, fontWeight: 700, fontSize: '11px' } : undefined,
    labelBgStyle: { fill: '#fff', fillOpacity: 0.9 },
  })
  pushSnapshot()
}

// ---- Selection ----

function onNodeClick({ node }: { node: Node }) { selectedNode.value = node; selectedEdge.value = null }
function onEdgeClick({ edge }: { edge: Edge }) { selectedEdge.value = edge; selectedNode.value = null }
function onPaneClick() { selectedNode.value = null; selectedEdge.value = null }

function deleteSelectedEdge() {
  const cur = selectedEdge.value
  if (!cur || readonly.value) return
  const dropId = cur.id
  const next = (edges.value as unknown as Edge[]).filter(e => e.id !== dropId)
  edges.value = next as typeof edges.value
  selectedEdge.value = null
  pushSnapshot()
}

function onKeyDown(event: KeyboardEvent) {
  if (readonly.value) return
  if (event.key === 'Delete' || event.key === 'Backspace') {
    if (selectedEdge.value) deleteSelectedEdge()
    else if (selectedNode.value) handleDeleteNode()
  }
}

// ---- Delete node ----

function handleDeleteNode() {
  if (!selectedNode.value || readonly.value) return
  const nodeId = selectedNode.value.id
  const label = selectedNode.value.data.label
  Modal.confirm({
    title: '删除节点',
    content: `确定删除节点 "${label}"？该节点的所有入边和出边将被一并删除。`,
    okText: '删除',
    okButtonProps: { status: 'danger' },
    onOk: () => {
      nodes.value = nodes.value.filter(n => n.id !== nodeId)
      edges.value = edges.value.filter(e => e.source !== nodeId && e.target !== nodeId)
      selectedNode.value = null
      pushSnapshot()
    },
  })
}

// ---- Drag & Drop ----

function onDragStart(event: DragEvent, nt: { type: string; label: string }) {
  if (readonly.value) return
  event.dataTransfer?.setData('nodeType', nt.type)
}

const { screenToFlowCoordinate } = useVueFlow()

function onDrop(event: DragEvent) {
  if (readonly.value) return
  const type = event.dataTransfer?.getData('nodeType')
  if (!type) return
  const position = screenToFlowCoordinate({ x: event.clientX, y: event.clientY })
  const id = `node_${++nodeCounter}`
  const color = nodeTypes.find(n => n.type === type)?.color || '#86909C'
  const baseData: Record<string, unknown> = {
    label: `${id} (${type})`, nodeType: type, color, dangling: false, configError: false,
    config: getDefaultConfig(type), contextJson: '{}',
    taskId: null, taskSnapshot: null, formFields: [],
    subWorkflowMetaId: null, subWorkflowVersion: null, subWorkflowMeta: null, subWorkflowVersions: [],
  }
  if (type === 'Parallel') Object.assign(baseData, parallelNodeDataDefaults())
  const newNode = {
    id,
    type: getVueFlowNodeType(type),
    position,
    data: baseData as unknown as Node['data'],
  }
  ;(nodes.value as unknown as Node[]).push(newNode as Node)
  pushSnapshot()
}

// ---- Save validation ----

function findDanglingNodes(): string[] {
  if (nodes.value.length <= 1) return []
  const connected = new Set<string>()
  const edgeList = edges.value as unknown as Edge[]
  for (const e of edgeList) {
    connected.add(e.source)
    connected.add(e.target)
  }
  const nodeList = nodes.value as unknown as Node[]
  return nodeList.filter(n => !connected.has(n.id)).map(n => n.id)
}

interface ValidationError { nodeId: string; message: string }

function validateNodeConfigs(): ValidationError[] {
  const errors: ValidationError[] = []
  for (const n of nodes.value) {
    const d = n.data as any
    const type = d.nodeType
    if (isTaskRefNode(type) && !d.taskId) {
      errors.push({ nodeId: n.id, message: `${n.id}: 未选择任务模板` })
    }
    if (type === 'SubWorkflow' && (!d.subWorkflowMetaId || !d.subWorkflowVersion)) {
      errors.push({ nodeId: n.id, message: `${n.id}: 未选择工作流或版本` })
    }
    if (type === 'IfCondition' && !d.config?.condition) {
      errors.push({ nodeId: n.id, message: `${n.id}: 条件表达式为空` })
    }
    if (type === 'IfCondition') {
      if (!edges.value.some(e => e.source === n.id && e.sourceHandle === 'then'))
        errors.push({ nodeId: n.id, message: `${n.id}: Then 分支未连接` })
      if (!edges.value.some(e => e.source === n.id && e.sourceHandle === 'else'))
        errors.push({ nodeId: n.id, message: `${n.id}: Else 分支未连接` })
    }
    if (type === 'ContextRewrite' && !d.config?.script) {
      errors.push({ nodeId: n.id, message: `${n.id}: 脚本为空` })
    }
    if (type === 'Parallel') {
      if (!d.config?.items_path?.trim()) {
        errors.push({ nodeId: n.id, message: `${n.id}: items_path 为空` })
      }
      const innerKind = d.parallelInnerKind || detectParallelInnerKind(d.config?.task_template)
      if (innerKind === 'Http') {
        const url = d.config?.task_template?.Http?.url
        if (!d.parallelInnerTaskId && (!url || String(url).trim() === '')) {
          errors.push({ nodeId: n.id, message: `${n.id}: 请为并发容器选择 HTTP 子任务模板` })
        }
      } else if (innerKind === 'Grpc') {
        if (!d.parallelInnerTaskId) {
          errors.push({ nodeId: n.id, message: `${n.id}: 请为并发容器选择 gRPC 子任务模板` })
        }
      } else if (innerKind === 'SubWorkflow') {
        if (!d.parallelSubWorkflowMetaId || d.parallelSubWorkflowVersion == null) {
          errors.push({ nodeId: n.id, message: `${n.id}: 请为并发容器选择子工作流及版本` })
        }
      }
    }
    if (type === 'ForkJoin') {
      try { const t = JSON.parse(d.config?.tasksJson || '[]'); if (!Array.isArray(t) || t.length === 0) throw 0 }
      catch { errors.push({ nodeId: n.id, message: `${n.id}: 子任务列表为空或格式错误` }) }
    }
  }
  return errors
}

function detectCycle(): string[] | null {
  const adj = new Map<string, string[]>()
  for (const n of nodes.value) adj.set(n.id, [])
  for (const e of edges.value) adj.get(e.source)?.push(e.target)

  const WHITE = 0, GRAY = 1, BLACK = 2
  const color = new Map<string, number>()
  for (const n of nodes.value) color.set(n.id, WHITE)
  const parent = new Map<string, string | null>()

  for (const n of nodes.value) {
    if (color.get(n.id) !== WHITE) continue
    const stack: string[] = [n.id]
    while (stack.length > 0) {
      const u = stack[stack.length - 1]
      if (color.get(u) === WHITE) {
        color.set(u, GRAY)
        for (const v of adj.get(u) || []) {
          if (color.get(v) === GRAY) {
            const cycle = [v, u]
            let cur = u
            while (cur !== v) { cur = parent.get(cur)!; if (cur) cycle.push(cur) }
            return cycle.reverse()
          }
          if (color.get(v) === WHITE) { parent.set(v, u); stack.push(v) }
        }
      } else {
        color.set(u, BLACK)
        stack.pop()
      }
    }
  }
  return null
}

function markNodes(nodeIds: string[], type: 'dangling' | 'configError') {
  const s = new Set(nodeIds)
  for (const n of nodes.value) {
    n.data = { ...n.data, [type]: s.has(n.id), ...(type === 'dangling' ? {} : {}) }
  }
}

function clearMarks() {
  for (const n of nodes.value) {
    n.data = { ...n.data, dangling: false, configError: false }
  }
}

// ---- Build & Save ----

function computeEntryNodeId(): string {
  const nodeList = nodes.value as unknown as Node[]
  if (nodeList.length === 0) return ''
  const inDegree = new Map<string, number>()
  for (const n of nodeList) inDegree.set(n.id, 0)
  const edgeList = edges.value as unknown as Edge[]
  for (const e of edgeList) {
    inDegree.set(e.target, (inDegree.get(e.target) || 0) + 1)
  }
  const candidate = nodeList.find(n => (inDegree.get(n.id) || 0) === 0)
  return candidate?.id || nodeList[0].id
}

function buildWorkflowEntity(): any {
  const nextNodeMap = new Map<string, string>()
  const thenMap = new Map<string, string>()
  const elseMap = new Map<string, string>()
  for (const e of edges.value) {
    if (e.sourceHandle === 'then') thenMap.set(e.source, e.target)
    else if (e.sourceHandle === 'else') elseMap.set(e.source, e.target)
    else nextNodeMap.set(e.source, e.target)
  }

  const workflowNodes: any[] = (nodes.value as any[]).map((n: any) => {
    const d = n.data as any
    let config: any
    const taskId = d.taskId || (d.nodeType === 'Parallel' ? d.parallelInnerTaskId : null) || null

    if (isTaskRefNode(d.nodeType) && d.taskSnapshot) {
      config = d.taskSnapshot
    } else if (d.nodeType === 'SubWorkflow') {
      config = { SubWorkflow: { workflow_meta_id: d.subWorkflowMetaId || '', workflow_version: d.subWorkflowVersion || 1, form: formFieldsToFormArray(d.formFields || []), timeout: d.config.timeout || null } }
    } else {
      switch (d.nodeType) {
        case 'IfCondition':
          config = { IfCondition: { name: d.config.name, condition: d.config.condition, then_task: thenMap.get(n.id) || null, else_task: elseMap.get(n.id) || null } }; break
        case 'ContextRewrite':
          config = { ContextRewrite: { name: d.config.name, script: d.config.script, merge_mode: d.config.merge_mode || 'Merge' } }; break
        case 'Parallel':
          config = {
            Parallel: {
              items_path: d.config.items_path,
              item_alias: d.config.item_alias,
              task_template: buildParallelTaskTemplateForSave(d),
              concurrency: d.config.concurrency || 10,
              mode: d.config.mode || 'Rolling',
              max_failures: d.config.max_failures,
            },
          }
          break
        case 'ForkJoin': {
          let tasks: any[] = []; try { tasks = JSON.parse(d.config.tasksJson || '[]') } catch {}
          config = { ForkJoin: { tasks, concurrency: d.config.concurrency || 5, mode: d.config.mode || 'Rolling', max_failures: d.config.max_failures } }; break
        }
        case 'Pause':
          config = { Pause: { wait_seconds: d.config.wait_seconds || 10, mode: d.config.mode || 'Auto' } }; break
        default: config = d.nodeType
      }
    }
    if (isTaskRefNode(d.nodeType) && d.formFields?.length) {
      const formData = formFieldsToFormArray(d.formFields)
      const typeKey = d.nodeType as string
      if (config?.[typeKey]) config[typeKey] = { ...config[typeKey], form: formData }
    }
    let context: any = {}; try { context = JSON.parse(d.contextJson || '{}') } catch {}
    return { node_id: n.id, node_type: d.nodeType, task_id: taskId, config, context, next_node: d.nodeType === 'IfCondition' ? null : (nextNodeMap.get(n.id) || null) }
  })
  return {
    workflow_meta_id: metaId,
    version: currentVersion.value || 1,
    status: 'Draft',
    entry_node: computeEntryNodeId(),
    nodes: workflowNodes,
  }
}

async function handleSave() {
  if (readonly.value) return
  clearMarks()

  if (nodes.value.length === 0) {
    Notification.warning({ content: '画布为空，请添加至少一个节点' }); return
  }

  const danglingIds = findDanglingNodes()
  if (danglingIds.length > 0) {
    markNodes(danglingIds, 'dangling')
    Notification.warning({ title: '存在未连接的节点', content: `请连接或删除：${danglingIds.join(', ')}`, duration: 5000 }); return
  }

  const configErrors = validateNodeConfigs()
  if (configErrors.length > 0) {
    markNodes(configErrors.map(e => e.nodeId), 'configError')
    Notification.warning({ title: '节点配置不完整', content: configErrors.map(e => e.message).join('\n'), duration: 5000 }); return
  }

  const cycle = detectCycle()
  if (cycle) {
    Notification.error({ title: '检测到回环', content: `回环路径：${cycle.join(' → ')}`, duration: 5000 }); return
  }

  saving.value = true
  try {
    const entity = buildWorkflowEntity()
    await workflowApi.saveTemplate(metaId, entity)
    currentVersion.value = entity.version
    if (!versionStatus.value) versionStatus.value = 'Draft'
    Notification.success({ content: '保存成功' })
  } catch {} finally { saving.value = false }
}

// ---- Load from entity ----

async function loadFromEntity(entity: any) {
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'LR', nodesep: 60, ranksep: 100 })
  g.setDefaultEdgeLabel(() => ({}))
  for (const n of entity.nodes) {
    g.setNode(n.node_id, { width: 160, height: 44 })
    if (n.next_node) g.setEdge(n.node_id, n.next_node)
    if (n.config?.IfCondition) {
      if (n.config.IfCondition.then_task) g.setEdge(n.node_id, n.config.IfCondition.then_task)
      if (n.config.IfCondition.else_task) g.setEdge(n.node_id, n.config.IfCondition.else_task)
    }
  }
  dagre.layout(g)
  nodeCounter = entity.nodes.reduce((max: number, n: any) => {
    const m = n.node_id?.match(/^node_(\d+)$/)
    return m ? Math.max(max, parseInt(m[1], 10)) : max
  }, 0)

  nodes.value = entity.nodes.map((n: any) => {
    const pos = g.node(n.node_id)
    const type = n.node_type
    const color = nodeTypes.find(t => t.type === type)?.color || '#86909C'
    let config: any = getDefaultConfig(type)
    let taskId: string | null = n.task_id || null
    let taskSnapshot: any = null
    let formFields: EditorFormField[] = []
    let subWorkflowMetaId: string | null = null
    let subWorkflowVersion: number | null = null
    let subWorkflowMeta: any = null

    if (isTaskRefNode(type) && n.config) {
      taskSnapshot = n.config
      const inner = (n.config as any)[type]
      if (inner?.form) formFields = buildFormFields(inner.form)
    } else if (type === 'SubWorkflow' && n.config?.SubWorkflow) {
      const s = n.config.SubWorkflow
      subWorkflowMetaId = s.workflow_meta_id; subWorkflowVersion = s.workflow_version
      config = { timeout: s.timeout }
      if (s.form?.length) formFields = buildFormFields(s.form)
      subWorkflowMeta = workflowMetas.value.find(m => m.workflow_meta_id === subWorkflowMetaId) || null
    } else if (type === 'IfCondition' && n.config?.IfCondition) {
      config = { name: n.config.IfCondition.name, condition: n.config.IfCondition.condition }
    } else if (type === 'ContextRewrite' && n.config?.ContextRewrite) {
      config = { ...n.config.ContextRewrite }
    } else if (type === 'Parallel' && n.config?.Parallel) {
      const p = n.config.Parallel
      config = { items_path: p.items_path, item_alias: p.item_alias, concurrency: p.concurrency, mode: p.mode, max_failures: p.max_failures, task_template: p.task_template }
    } else if (type === 'ForkJoin' && n.config?.ForkJoin) {
      const f = n.config.ForkJoin
      config = { concurrency: f.concurrency, mode: f.mode, max_failures: f.max_failures, tasksJson: JSON.stringify(f.tasks, null, 2) }
    } else if (type === 'Pause' && n.config?.Pause) {
      config = { wait_seconds: n.config.Pause.wait_seconds, mode: n.config.Pause.mode }
    }

    const data: Record<string, unknown> = {
      label: '', nodeType: type, color, dangling: false, configError: false, config,
      contextJson: JSON.stringify(n.context || {}, null, 2),
      taskId, taskSnapshot, formFields, subWorkflowMetaId, subWorkflowVersion, subWorkflowMeta, subWorkflowVersions: [],
    }
    if (type === 'Parallel') {
      Object.assign(data, parallelNodeDataDefaults())
      hydrateParallelEditorState(data, taskCache.value, workflowMetas.value)
      if (taskId && !data.parallelInnerTaskId) data.parallelInnerTaskId = taskId
    }
    data.label = resolveLabel(n.node_id, type, data)

    return { id: n.node_id, type: getVueFlowNodeType(type), position: { x: pos?.x || 0, y: pos?.y || 0 }, data }
  })

  for (const n of nodes.value) {
    const d = n.data as Record<string, unknown>
    if (d.nodeType === 'Parallel' && d.parallelInnerKind === 'SubWorkflow' && d.parallelSubWorkflowMetaId) {
      await refreshParallelSubWorkflowVersions(d)
    }
  }

  const loadedEdges: Edge[] = []
  for (const n of entity.nodes) {
    if (n.next_node) loadedEdges.push({ id: `${n.node_id}-next->${n.next_node}`, source: n.node_id, target: n.next_node, type: 'smoothstep' })
    if (n.config?.IfCondition) {
      const ic = n.config.IfCondition
      if (ic.then_task) loadedEdges.push({ id: `${n.node_id}-then->${ic.then_task}`, source: n.node_id, target: ic.then_task, sourceHandle: 'then', type: 'smoothstep', style: { stroke: '#00B42A', strokeWidth: 2 }, label: 'True', labelStyle: { fill: '#00B42A', fontWeight: 700, fontSize: '11px' }, labelBgStyle: { fill: '#fff', fillOpacity: 0.9 } })
      if (ic.else_task) loadedEdges.push({ id: `${n.node_id}-else->${ic.else_task}`, source: n.node_id, target: ic.else_task, sourceHandle: 'else', type: 'smoothstep', style: { stroke: '#F53F3F', strokeWidth: 2 }, label: 'False', labelStyle: { fill: '#F53F3F', fontWeight: 700, fontSize: '11px' }, labelBgStyle: { fill: '#fff', fillOpacity: 0.9 } })
    }
  }
  edges.value = loadedEdges
  pushSnapshot()
}

// ---- Init ----

onMounted(async () => {
  try {
    const [metaRes, tasksRes, metasRes] = await Promise.all([
      workflowApi.getMeta(metaId), taskApi.list(), workflowApi.listMeta(),
    ])
    metaName.value = metaRes.data.name
    taskCache.value = tasksRes.data
    workflowMetas.value = metasRes.data
  } catch {}

  if (versionParam) {
    try {
      const res = await workflowApi.getTemplate(metaId, versionParam)
      versionStatus.value = res.data.status
      await loadFromEntity(res.data)
    } catch {}
  } else {
    try {
      const res = await workflowApi.listTemplates(metaId)
      const nextVersion = res.data.length > 0 ? Math.max(...res.data.map((v: any) => v.version)) + 1 : 1
      currentVersion.value = nextVersion
      versionStatus.value = 'Draft'
    } catch { currentVersion.value = 1; versionStatus.value = 'Draft' }
    pushSnapshot()
  }
})
</script>

<style scoped>
.editor-container { display: flex; flex-direction: column; height: calc(100vh - 88px); }
.editor-header { display: flex; align-items: center; gap: 12px; padding: 8px 12px; border-bottom: 1px solid var(--color-border); background: var(--color-bg-2); }
.editor-title { flex: 1; font-size: 16px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
.editor-body { display: flex; flex: 1; overflow: hidden; }
.editor-panel-left { width: 180px; border-right: 1px solid var(--color-border); background: var(--color-bg-2); padding: 12px; overflow-y: auto; }
.editor-canvas { flex: 1; position: relative; }
.editor-panel-right { width: 320px; border-left: 1px solid var(--color-border); background: var(--color-bg-2); padding: 12px; overflow-y: auto; }
.panel-title { font-weight: 600; margin-bottom: 12px; font-size: 14px; }
.node-item { padding: 8px 12px; margin-bottom: 6px; border-radius: 4px; border-left: 3px solid; background: var(--color-fill-2); font-size: 13px; display: flex; align-items: center; gap: 8px; }
.node-item:hover { background: var(--color-fill-3); }
.node-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.task-info { margin-bottom: 8px; }
.form-list { display: flex; flex-direction: column; gap: 8px; width: 100%; }
.form-row { display: flex; gap: 6px; align-items: flex-start; }
.shortcut-hint { font-size: 11px; color: var(--color-text-3); padding: 4px 8px; background: rgba(255,255,255,0.8); border-radius: 4px; user-select: none; pointer-events: none; }
</style>

<style>
@import '@vue-flow/core/dist/style.css';
@import '@vue-flow/core/dist/theme-default.css';
@import '@vue-flow/controls/dist/style.css';
@import '@vue-flow/minimap/dist/style.css';
</style>
