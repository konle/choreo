<template>
  <div>
    <a-divider>子任务列表</a-divider>
    <div v-for="(item, index) in items" :key="index" class="fj-task-item">
      <div class="fj-task-header">
        <a-input v-model="item.task_key" :disabled="readonly" placeholder="task_key" style="width: 120px" @change="emit('change')" />
        <a-input v-model="item.name" :disabled="readonly" placeholder="名称" style="width: 100px; margin-left: 6px" @change="emit('change')" />
        <a-select :model-value="item.kind" :disabled="readonly" style="width: 110px; margin-left: 6px" @update:model-value="onKindChange(index, $event as ForkJoinTaskKind)">
          <a-option value="Http">HTTP</a-option>
          <a-option value="Llm">LLM</a-option>
          <a-option value="Grpc">gRPC</a-option>
          <a-option value="SubWorkflow">子工作流</a-option>
        </a-select>
        <a-button v-if="!readonly" type="text" status="danger" size="mini" style="margin-left: 4px" @click="removeItem(index)">✕</a-button>
      </div>

      <div class="fj-task-body">
        <PublishedTaskRefFields
          v-if="item.kind === 'Http' || item.kind === 'Grpc' || item.kind === 'Llm'"
          section-title=""
          :task-type="item.kind"
          v-model:task-id="item.task_id"
          v-model:form-fields="item.formFields"
          :tasks="getTasksForKind(item.kind)"
          :task-snapshot="item.taskSnapshot"
          :readonly="readonly"
          @change="onTaskChange(index)"
          @update:form-fields="emit('change')"
        />

        <SubworkflowRefFields
          v-else
          section-title=""
          v-model:meta-id="item.subWorkflowMetaId"
          v-model:version="item.subWorkflowVersion"
          v-model:timeout="item.subWorkflowTimeout"
          v-model:form-fields="item.subWorkflowFormFields"
          :sub-workflow-meta="item.subWorkflowMeta"
          :sub-workflow-versions="item.subWorkflowVersions"
          :workflow-metas="workflowMetas"
          :readonly="readonly"
          @meta-change="onSubMetaChange(index)"
          @version-change="emit('change')"
          @update:timeout="emit('change')"
          @update:form-fields="emit('change')"
        />
      </div>
    </div>
    <a-button v-if="!readonly" type="dashed" long @click="addItem">+ 添加子任务</a-button>
  </div>
</template>

<script setup lang="ts">
import { type Ref } from 'vue'
import type { TaskEntity } from '../../../types/task'
import type { WorkflowMetaEntity } from '../../../types/workflow'
import { workflowApi } from '../../../api/workflow'
import PublishedTaskRefFields from './published-task-ref-fields.vue'
import SubworkflowRefFields from './subworkflow-ref-fields.vue'
import { buildFormFields } from './workflow-editor-form-utils'
import {
  type ForkJoinTaskItemEditor,
  type ForkJoinTaskKind,
  createEmptyForkJoinTaskItem,
  defaultForkJoinTaskTemplate,
} from './forkjoin-task-utils'

const props = defineProps<{
  items: ForkJoinTaskItemEditor[]
  taskCache: TaskEntity[]
  workflowMetas: WorkflowMetaEntity[]
  readonly?: boolean
}>()

const emit = defineEmits<{ change: [] }>()

function getTasksForKind(kind: ForkJoinTaskKind): TaskEntity[] {
  return props.taskCache.filter(t => t.task_type === kind && t.status === 'Published')
}

function addItem() {
  props.items.push(createEmptyForkJoinTaskItem(props.items.length))
  emit('change')
}

function removeItem(index: number) {
  props.items.splice(index, 1)
  for (let i = 0; i < props.items.length; i++) {
    props.items[i].task_key = `task_${i + 1}`
  }
  emit('change')
}

function onKindChange(index: number, newKind: ForkJoinTaskKind) {
  const item = props.items[index]
  item.kind = newKind
  item.taskTemplate = defaultForkJoinTaskTemplate(newKind)
  item.taskSnapshot = defaultForkJoinTaskTemplate(newKind)
  item.task_id = null
  item.formFields = []
  item.subWorkflowMetaId = null
  item.subWorkflowVersion = null
  item.subWorkflowMeta = null
  item.subWorkflowVersions = []
  item.subWorkflowFormFields = []
  item.subWorkflowTimeout = null
  emit('change')
}

function onTaskChange(index: number) {
  const item = props.items[index]
  const id = item.task_id
  if (!id) {
    item.taskSnapshot = null
    item.formFields = []
    item.taskTemplate = defaultForkJoinTaskTemplate(item.kind)
    emit('change')
    return
  }
  const task = props.taskCache.find(t => t.id === id)
  if (!task) {
    item.taskSnapshot = null
    item.formFields = []
    emit('change')
    return
  }
  item.taskSnapshot = task.task_template as Record<string, unknown>
  item.taskTemplate = JSON.parse(JSON.stringify(task.task_template)) as typeof item.taskTemplate
  const tpl = task.task_template as Record<string, unknown>
  if (tpl !== null && item.kind in tpl) {
    const inner = (tpl as Record<string, { form?: unknown[] }>)[item.kind]
    item.formFields = inner?.form?.length ? buildFormFields(inner.form as Parameters<typeof buildFormFields>[0]) : []
  } else {
    item.formFields = []
  }
  emit('change')
}

async function onSubMetaChange(index: number) {
  const item = props.items[index]
  const id = item.subWorkflowMetaId
  if (!id) {
    item.subWorkflowMeta = null
    item.subWorkflowVersions = []
    item.subWorkflowVersion = null
    item.subWorkflowFormFields = []
    item.taskTemplate = defaultForkJoinTaskTemplate('SubWorkflow')
    emit('change')
    return
  }
  const meta = props.workflowMetas.find(m => m.workflow_meta_id === id)
  item.subWorkflowMeta = meta || null
  try {
    const res = await workflowApi.listTemplates(id)
    item.subWorkflowVersions = res.data
    if (res.data.length > 0) item.subWorkflowVersion = res.data[res.data.length - 1].version
  } catch {
    item.subWorkflowVersions = []
  }
  item.subWorkflowFormFields = meta?.form?.length ? buildFormFields(meta.form) : []
  item.taskTemplate = {
    SubWorkflow: {
      workflow_meta_id: id,
      workflow_version: item.subWorkflowVersion || 1,
      form: [],
      timeout: item.subWorkflowTimeout ?? null,
    },
  }
  emit('change')
}
</script>

<style scoped>
.fj-task-item {
  border: 1px solid var(--color-border-2);
  border-radius: 4px;
  padding: 8px;
  margin-bottom: 8px;
}
.fj-task-header {
  display: flex;
  align-items: center;
  margin-bottom: 6px;
}
.fj-task-body {
  margin-top: 4px;
}
</style>