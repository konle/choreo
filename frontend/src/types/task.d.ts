import type { JsonValue } from './common'

export type TaskType = 'Http' | 'IfCondition' | 'ContextRewrite' | 'Parallel' | 'ForkJoin' | 'SubWorkflow' | 'Grpc' | 'Approval' | 'Pause' | 'Llm'
export type TaskStatus = 'Draft' | 'Published'
export type TaskInstanceStatus = 'Pending' | 'Running' | 'Completed' | 'Failed' | 'Canceled' | 'Skipped'
export type HttpMethod = 'Get' | 'Post' | 'Put' | 'Delete' | 'Head'
export type ParallelMode = 'Rolling' | 'Batch'
export type MergeMode = 'Merge' | 'Replace'

export type FormValueType = 'String' | 'Number' | 'Bool' | 'Json' | 'Variable'

export interface FormField {
  key: string
  value: string | number | boolean | JsonValue
  type: FormValueType
  description?: string
}

export interface TaskHttpTemplate {
  url: string
  method: HttpMethod
  headers: FormField[]
  body: FormField[]
  form: FormField[]
  retry_count: number
  retry_delay: number
  timeout: number
  success_condition: string | null
}

export interface IfConditionTemplate {
  condition: string
  name: string
  then_task: string | null
  else_task: string | null
}

export interface ContextRewriteTemplate {
  name: string
  script: string
  merge_mode: MergeMode
}

export interface ParallelTemplate {
  items_path: string
  item_alias: string
  task_template: TaskTemplate
  concurrency: number
  mode: ParallelMode
  max_failures: number | null
}

export interface ForkJoinTaskItem {
  task_key: string
  task_id?: string | null
  name: string
  task_template: TaskTemplate
}

export interface ForkJoinTemplate {
  tasks: ForkJoinTaskItem[]
  concurrency: number
  mode: ParallelMode
  max_failures: number | null
}

export interface SubWorkflowTemplate {
  workflow_meta_id: string
  workflow_version: number
  form: FormField[]
  timeout: number | null
}

export type ApprovalMode = 'Any' | 'All' | 'Majority'
export type SelfApprovalPolicy = 'Allow' | 'Skip'

export interface ApproverRule {
  User?: string
  Role?: string
  ContextVariable?: string
}

export interface ApprovalTemplate {
  name: string
  title: string
  description: string | null
  approvers: ApproverRule[]
  approval_mode: ApprovalMode
  timeout: number | null
  self_approval?: SelfApprovalPolicy
}

export type PauseMode = 'Auto' | 'Manual'

export interface PauseTemplate {
  wait_seconds: number
  mode: PauseMode
}

export type LlmResponseFormat = 'Text' | 'JsonObject'

export interface LlmTemplate {
  base_url: string
  model: string
  api_key_ref: string
  system_prompt: string | null
  user_prompt: string
  temperature: number | null
  max_tokens: number | null
  timeout: number
  retry_count: number
  retry_delay: number
  response_format: LlmResponseFormat | null
  form: FormField[]
}

export type TaskTemplate =
  | { Http: TaskHttpTemplate }
  | { IfCondition: IfConditionTemplate }
  | { ContextRewrite: ContextRewriteTemplate }
  | { Parallel: ParallelTemplate }
  | { ForkJoin: ForkJoinTemplate }
  | { SubWorkflow: SubWorkflowTemplate }
  | { Approval: ApprovalTemplate }
  | { Pause: PauseTemplate }
  | { Llm: LlmTemplate }
  | 'Grpc'

export interface TaskEntity {
  id: string
  tenant_id: string
  name: string
  task_type: TaskType
  task_template: TaskTemplate
  description: string
  status: TaskStatus
  created_at: string
  updated_at: string
  deleted_at: string | null
}

export interface WorkflowCallerContext {
  workflow_instance_id: string
  node_id: string
  parent_task_instance_id: string | null
  item_index: number | null
}

export interface TaskInstanceEntity {
  id: string
  tenant_id: string
  task_id: string
  task_name: string
  task_type: TaskType
  task_template: TaskTemplate
  task_status: TaskInstanceStatus
  task_instance_id: string
  created_at: string
  updated_at: string
  deleted_at: string | null
  input: JsonValue | null
  output: JsonValue | null
  error_message: string | null
  execution_duration: number | null
  caller_context: WorkflowCallerContext | null
}

export interface ListTaskInstancesParams {
  page?: number
  page_size?: number
  task_id?: string
  status?: TaskInstanceStatus
  sort_by?: string
  sort_order?: string
}
