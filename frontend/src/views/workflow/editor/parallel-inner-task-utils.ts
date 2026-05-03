import type { TaskEntity, TaskHttpTemplate, TaskTemplate } from '../../../types/task'
import type { EditorFormField } from './workflow-editor-form-utils'
import { buildFormFields, formFieldsToFormArray } from './workflow-editor-form-utils'
import type { WorkflowMetaEntity } from '../../../types/workflow'

export type ParallelInnerKind = 'Http' | 'Grpc' | 'SubWorkflow' | 'Llm'

export function defaultParallelInnerTemplate(kind: ParallelInnerKind): TaskTemplate {
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
      return defaultParallelInnerTemplate('Http')
  }
}

export function detectParallelInnerKind(tt: TaskTemplate | null | undefined): ParallelInnerKind {
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

/** Hydrate editor-only Parallel fields from `config.task_template` + caches. */
export function hydrateParallelEditorState(
  data: Record<string, unknown>,
  taskCache: TaskEntity[],
  workflowMetas: WorkflowMetaEntity[],
): void {
  const config = data.config as Record<string, unknown>
  const tt = config.task_template as TaskTemplate | null | undefined
  const kind = detectParallelInnerKind(tt)
  data.parallelInnerKind = kind

  data.parallelInnerTaskId = null
  data.parallelInnerSnapshot = null
  data.parallelInnerFormFields = [] as EditorFormField[]

  data.parallelSubWorkflowMetaId = null
  data.parallelSubWorkflowVersion = null
  data.parallelSubWorkflowMeta = null
  data.parallelSubWorkflowVersions = [] as unknown[]
  data.parallelSubWorkflowFormFields = [] as EditorFormField[]
  data.parallelSubWorkflowTimeout = null

  if (kind === 'Http' && tt && typeof tt === 'object' && 'Http' in tt) {
    const http = (tt as { Http: { form?: unknown[] } }).Http
    data.parallelInnerSnapshot = tt
    data.parallelInnerFormFields = http?.form?.length ? buildFormFields(http.form as Parameters<typeof buildFormFields>[0]) : []
    const match = taskCache.find(
      t =>
        t.task_type === 'Http' &&
        t.status === 'Published' &&
        JSON.stringify(t.task_template) === JSON.stringify(tt),
    )
    if (match) data.parallelInnerTaskId = match.id
  } else if (kind === 'Llm' && tt && typeof tt === 'object' && 'Llm' in tt) {
    data.parallelInnerSnapshot = tt
    const match = taskCache.find(
      t =>
        t.task_type === 'Llm' &&
        t.status === 'Published' &&
        JSON.stringify(t.task_template) === JSON.stringify(tt),
    )
    if (match) data.parallelInnerTaskId = match.id
  } else if (kind === 'Grpc') {
    data.parallelInnerSnapshot = tt
    const match = taskCache.find(
      t =>
        t.task_type === 'Grpc' &&
        t.status === 'Published' &&
        JSON.stringify(t.task_template) === JSON.stringify(tt),
    )
    if (match) data.parallelInnerTaskId = match.id
  } else if (kind === 'SubWorkflow' && tt && typeof tt === 'object' && 'SubWorkflow' in tt) {
    const sw = (tt as { SubWorkflow: { workflow_meta_id: string; workflow_version: number; form?: unknown[]; timeout?: number | null } }).SubWorkflow
    data.parallelSubWorkflowMetaId = sw.workflow_meta_id
    data.parallelSubWorkflowVersion = sw.workflow_version
    data.parallelSubWorkflowTimeout = sw.timeout ?? null
    if (sw.form?.length) data.parallelSubWorkflowFormFields = buildFormFields(sw.form as Parameters<typeof buildFormFields>[0])
    data.parallelSubWorkflowMeta = workflowMetas.find(m => m.workflow_meta_id === sw.workflow_meta_id) || null
  }
}

export function buildParallelTaskTemplateForSave(data: Record<string, unknown>): TaskTemplate {
  const kind = (data.parallelInnerKind || 'Http') as ParallelInnerKind
  if (kind === 'SubWorkflow') {
    return {
      SubWorkflow: {
        workflow_meta_id: (data.parallelSubWorkflowMetaId as string) || '',
        workflow_version: (data.parallelSubWorkflowVersion as number) || 1,
        form: formFieldsToFormArray((data.parallelSubWorkflowFormFields as EditorFormField[]) || []),
        timeout: (data.parallelSubWorkflowTimeout as number | null | undefined) ?? null,
      },
    }
  }

  const cfg = data.config as { task_template?: TaskTemplate } | undefined
  let tt: TaskTemplate | null | undefined = cfg?.task_template
  if (tt == null) tt = defaultParallelInnerTemplate(kind)

  if (kind === 'Http' && typeof tt === 'object' && tt !== null && 'Http' in tt) {
    const base = (tt as { Http: TaskHttpTemplate }).Http
    const fields = (data.parallelInnerFormFields as EditorFormField[]) || []
    return {
      Http: {
        ...base,
        form: fields.length ? formFieldsToFormArray(fields) : base.form,
      },
    }
  }

  if (kind === 'Llm' && typeof tt === 'object' && tt !== null && 'Llm' in tt) {
    return tt as TaskTemplate
  }

  if (kind === 'Grpc') {
    const raw = cfg?.task_template
    if (raw !== undefined && raw !== null) return raw as TaskTemplate
    return 'Grpc'
  }

  return defaultParallelInnerTemplate('Http')
}
