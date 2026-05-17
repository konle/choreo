import type { TaskEntity, TaskTemplate } from '../../../types/task'
import type { EditorFormField } from './workflow-editor-form-utils'
import { buildFormFields, formFieldsToFormArray } from './workflow-editor-form-utils'
import type { WorkflowMetaEntity } from '../../../types/workflow'

export type ForkJoinTaskKind = 'Http' | 'Llm' | 'Grpc' | 'SubWorkflow'

export interface ForkJoinTaskItemEditor {
  task_key: string
  task_id: string | null
  name: string
  kind: ForkJoinTaskKind
  taskTemplate: TaskTemplate | null
  taskSnapshot: Record<string, unknown> | string | null
  formFields: EditorFormField[]
  subWorkflowMetaId: string | null
  subWorkflowVersion: number | null
  subWorkflowMeta: WorkflowMetaEntity | null
  subWorkflowVersions: { version: number; nodes?: unknown[] }[]
  subWorkflowFormFields: EditorFormField[]
  subWorkflowTimeout: number | null
}

export function defaultForkJoinTaskTemplate(kind: ForkJoinTaskKind): TaskTemplate {
  switch (kind) {
    case 'Http':
      return {
        Http: {
          url: '',
          method: 'Get',
          headers: [],
          body: [],
          form: [],
          retry_count: 0,
          retry_delay: 0,
          timeout: 30,
          success_condition: null,
        },
      }
    case 'Llm':
      return {
        Llm: {
          base_url: '',
          model: '',
          api_key_ref: '',
          system_prompt: null,
          user_prompt: '',
          temperature: null,
          max_tokens: null,
          timeout: 60,
          retry_count: 0,
          retry_delay: 3,
          response_format: null,
          form: [],
        },
      }
    case 'Grpc':
      return 'Grpc'
    case 'SubWorkflow':
      return {
        SubWorkflow: {
          workflow_meta_id: '',
          workflow_version: 1,
          form: [],
          timeout: null,
        },
      }
    default:
      return defaultForkJoinTaskTemplate('Http')
  }
}

export function detectForkJoinTaskKind(tt: TaskTemplate | null | undefined): ForkJoinTaskKind {
  if (tt == null) return 'Http'
  if (typeof tt === 'string' && tt === 'Grpc') return 'Grpc'
  if (typeof tt === 'object' && tt !== null) {
    if ('Http' in tt) return 'Http'
    if ('Llm' in tt) return 'Llm'
    if ('SubWorkflow' in tt) return 'SubWorkflow'
    if ('Grpc' in tt) return 'Grpc'
  }
  return 'Http'
}

export function createEmptyForkJoinTaskItem(index: number): ForkJoinTaskItemEditor {
  return {
    task_key: `task_${index + 1}`,
    task_id: null,
    name: `Task ${index + 1}`,
    kind: 'Http',
    taskTemplate: defaultForkJoinTaskTemplate('Http'),
    taskSnapshot: null,
    formFields: [],
    subWorkflowMetaId: null,
    subWorkflowVersion: null,
    subWorkflowMeta: null,
    subWorkflowVersions: [],
    subWorkflowFormFields: [],
    subWorkflowTimeout: null,
  }
}

export function hydrateForkJoinEditorState(
  tasks: any[],
  taskCache: TaskEntity[],
  workflowMetas: WorkflowMetaEntity[],
): ForkJoinTaskItemEditor[] {
  return tasks.map((t: any) => {
    const kind = detectForkJoinTaskKind(t.task_template)
    const item: ForkJoinTaskItemEditor = {
      task_key: t.task_key || '',
      task_id: t.task_id || null,
      name: t.name || t.task_key || '',
      kind,
      taskTemplate: t.task_template,
      taskSnapshot: t.task_template,
      formFields: [],
      task_id: null,
      subWorkflowMetaId: null,
      subWorkflowVersion: null,
      subWorkflowMeta: null,
      subWorkflowVersions: [],
      subWorkflowFormFields: [],
      subWorkflowTimeout: null,
    }

    if ((kind === 'Http' || kind === 'Llm' || kind === 'Grpc') && t.task_template && typeof t.task_template === 'object') {
      const tpl = t.task_template as Record<string, unknown>
      const innerKey = kind
      const inner = tpl[kind] as { form?: unknown[] } | undefined
      item.formFields = inner?.form?.length ? buildFormFields(inner.form as Parameters<typeof buildFormFields>[0]) : []
      const match = taskCache.find(
        tc => tc.task_type === kind && tc.status === 'Published' && JSON.stringify(tc.task_template) === JSON.stringify(t.task_template),
      )
      if (match) item.task_id = match.id
    } else if (kind === 'SubWorkflow' && t.task_template && typeof t.task_template === 'object') {
      const sw = (t.task_template as { SubWorkflow: { workflow_meta_id: string; workflow_version: number; form?: unknown[]; timeout?: number | null } }).SubWorkflow
      item.subWorkflowMetaId = sw.workflow_meta_id
      item.subWorkflowVersion = sw.workflow_version
      item.subWorkflowTimeout = sw.timeout ?? null
      item.subWorkflowMeta = workflowMetas.find(m => m.workflow_meta_id === sw.workflow_meta_id) || null
      if (sw.form?.length) item.subWorkflowFormFields = buildFormFields(sw.form as Parameters<typeof buildFormFields>[0])
    }

    return item
  })
}

export function buildForkJoinTasksForSave(items: ForkJoinTaskItemEditor[]): any[] {
  return items.map((item) => {
    let taskTemplate: TaskTemplate

    if (item.kind === 'SubWorkflow') {
      taskTemplate = {
        SubWorkflow: {
          workflow_meta_id: item.subWorkflowMetaId || '',
          workflow_version: item.subWorkflowVersion || 1,
          form: formFieldsToFormArray(item.subWorkflowFormFields || []),
          timeout: item.subWorkflowTimeout ?? null,
        },
      }
    } else {
      if (item.taskSnapshot && typeof item.taskSnapshot === 'object') {
        taskTemplate = JSON.parse(JSON.stringify(item.taskSnapshot)) as TaskTemplate
        const typeKey = item.kind
        const tpl = (taskTemplate as Record<string, unknown>)[typeKey] as Record<string, unknown> | undefined
        if (tpl && item.formFields?.length) {
          tpl.form = formFieldsToFormArray(item.formFields)
        }
      } else {
        taskTemplate = defaultForkJoinTaskTemplate(item.kind)
      }
    }

    return {
      task_key: item.task_key,
      task_id: item.task_id || null,
      name: item.name,
      task_template: taskTemplate,
    }
  })
}