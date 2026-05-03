<template>
  <div>
    <a-page-header :title="isEdit ? '编辑任务模板' : '创建任务模板'" @back="$router.push('/tasks')" />

    <a-tabs :default-active-key="initialTab" @change="handleTabChange">
      <a-tab-pane key="config" title="模板配置">
        <a-card :loading="pageLoading">
          <a-form :model="form" layout="vertical" @submit-success="handleSave">
            <a-row :gutter="16">
              <a-col :span="12">
                <a-form-item field="name" label="名称" :rules="[{ required: true, message: '请输入名称' }]">
                  <a-input v-model="form.name" />
                </a-form-item>
              </a-col>
              <a-col :span="12">
                <a-form-item field="task_type" label="任务类型" :rules="[{ required: true, message: '请选择类型' }]">
                  <a-select v-model="form.task_type" :disabled="isEdit">
                    <a-option value="Http">HTTP</a-option>
                    <a-option value="Llm">LLM 大模型</a-option>
                    <a-option value="Grpc">gRPC</a-option>
                    <a-option value="Approval">审批</a-option>
                  </a-select>
                </a-form-item>
              </a-col>
            </a-row>
            <a-form-item field="description" label="描述">
              <a-textarea v-model="form.description" />
            </a-form-item>
            <a-form-item field="status" label="状态">
              <a-radio-group v-model="form.status">
                <a-radio value="Draft">草稿</a-radio>
                <a-radio value="Published">已发布</a-radio>
              </a-radio-group>
            </a-form-item>

            <!-- ==================== HTTP 配置 ==================== -->
            <template v-if="form.task_type === 'Http'">
              <a-divider>请求配置</a-divider>

              <!-- URL & Method -->
              <a-row :gutter="16">
                <a-col :span="16">
                  <a-form-item label="URL" required>
                    <a-input v-model="httpConfig.url" placeholder="https://example.com/api  支持 {{变量}} 模板" />
                  </a-form-item>
                </a-col>
                <a-col :span="8">
                  <a-form-item label="Method" required>
                    <a-select v-model="httpConfig.method">
                      <a-option value="Get">GET</a-option>
                      <a-option value="Post">POST</a-option>
                      <a-option value="Put">PUT</a-option>
                      <a-option value="Delete">DELETE</a-option>
                      <a-option value="Head">HEAD</a-option>
                    </a-select>
                  </a-form-item>
                </a-col>
              </a-row>

              <!-- Headers -->
              <a-form-item label="Headers">
                <div class="form-list">
                  <div class="preset-tags">
                    <span class="preset-label">常用：</span>
                    <a-tag v-for="p in headerPresets" :key="p.key" color="arcoblue" class="preset-tag" @click="addPresetHeader(p)">
                      {{ p.key }}
                    </a-tag>
                  </div>
                  <div v-for="(h, idx) in httpConfig.headers" :key="idx" class="form-row">
                    <a-auto-complete
                      v-model="h.key"
                      :data="headerSuggestions"
                      placeholder="Header 名称"
                      style="width: 200px"
                      allow-clear
                    />
                    <a-input v-model="h.value" placeholder="值 (Variable 类型支持 {{变量}})" style="flex: 1" />
                    <a-select v-model="h.type" style="width: 120px">
                      <a-option value="String">String</a-option>
                      <a-option value="Variable">Variable</a-option>
                    </a-select>
                    <a-input v-model="h.description" placeholder="描述 (可选)" style="width: 160px" />
                    <a-button status="danger" @click="httpConfig.headers.splice(idx, 1)">
                      <template #icon><icon-delete /></template>
                    </a-button>
                  </div>
                  <a-button type="dashed" long @click="addHeaderRow">+ 添加 Header</a-button>
                </div>
              </a-form-item>

              <!-- Body -->
              <a-form-item label="Body (请求体字段)">
                <div class="form-list">
                  <div v-for="(b, idx) in httpConfig.body" :key="idx" class="form-row">
                    <a-input v-model="b.key" placeholder="字段名" style="width: 160px" />
                    <a-input v-if="b.type === 'String' || b.type === 'Variable'" v-model="b.value" placeholder="值" style="flex: 1" />
                    <a-input-number v-else-if="b.type === 'Number'" v-model="b.value" placeholder="数值" style="flex: 1" />
                    <a-select v-else-if="b.type === 'Bool'" v-model="b.value" style="flex: 1">
                      <a-option :value="true">true</a-option>
                      <a-option :value="false">false</a-option>
                    </a-select>
                    <a-textarea v-else v-model="b.value" placeholder="JSON 值" :auto-size="{ minRows: 1, maxRows: 3 }" style="flex: 1" />
                    <a-select v-model="b.type" style="width: 120px">
                      <a-option value="String">String</a-option>
                      <a-option value="Number">Number</a-option>
                      <a-option value="Bool">Bool</a-option>
                      <a-option value="Json">Json</a-option>
                      <a-option value="Variable">Variable</a-option>
                    </a-select>
                    <a-input v-model="b.description" placeholder="描述 (可选)" style="width: 160px" />
                    <a-button status="danger" @click="httpConfig.body.splice(idx, 1)">
                      <template #icon><icon-delete /></template>
                    </a-button>
                  </div>
                  <a-button type="dashed" long @click="addBodyRow">+ 添加字段</a-button>
                </div>
              </a-form-item>

              <!-- 用户表单 -->
              <a-divider>用户表单定义</a-divider>
              <a-alert type="info" style="margin-bottom: 16px">
                此处定义的字段将在工作流实例创建时展示给用户填写，值可通过 <code>{<!-- -->{key}}</code> 在 URL / Headers / Body 中引用。
              </a-alert>
              <a-form-item label="Form (用户输入字段)">
                <div class="form-list">
                  <div v-for="(f, idx) in httpConfig.form" :key="idx" class="form-row">
                    <a-input v-model="f.key" placeholder="字段名 (key)" style="width: 160px" />
                    <a-input v-model="f.value" placeholder="默认值 (可选)" style="flex: 1" />
                    <a-select v-model="f.type" style="width: 120px">
                      <a-option value="String">String</a-option>
                      <a-option value="Number">Number</a-option>
                      <a-option value="Bool">Bool</a-option>
                      <a-option value="Json">Json</a-option>
                    </a-select>
                    <a-input v-model="f.description" placeholder="字段说明" style="width: 200px" />
                    <a-button status="danger" @click="httpConfig.form.splice(idx, 1)">
                      <template #icon><icon-delete /></template>
                    </a-button>
                  </div>
                  <a-button type="dashed" long @click="addFormRow">+ 添加表单字段</a-button>
                </div>
              </a-form-item>

              <!-- 运行参数 -->
              <a-divider>运行参数</a-divider>
              <a-row :gutter="16">
                <a-col :span="8">
                  <a-form-item label="超时 (秒)">
                    <a-input-number v-model="httpConfig.timeout" :min="1" :max="600" />
                  </a-form-item>
                </a-col>
                <a-col :span="8">
                  <a-form-item label="重试次数">
                    <a-input-number v-model="httpConfig.retry_count" :min="0" :max="10" />
                  </a-form-item>
                </a-col>
                <a-col :span="8">
                  <a-form-item label="重试延迟 (秒)">
                    <a-input-number v-model="httpConfig.retry_delay" :min="0" />
                  </a-form-item>
                </a-col>
              </a-row>
              <a-form-item label="成功条件">
                <a-input v-model="httpConfig.success_condition" placeholder="可选，如 status == 200" />
              </a-form-item>
            </template>

            <!-- ==================== LLM 配置 ==================== -->
            <template v-else-if="form.task_type === 'Llm'">
              <a-divider>模型配置</a-divider>
              <a-row :gutter="16">
                <a-col :span="12">
                  <a-form-item label="API Base URL" required>
                    <a-input v-model="llmConfig.base_url" placeholder="https://api.openai.com/v1" />
                  </a-form-item>
                </a-col>
                <a-col :span="12">
                  <a-form-item label="模型名称" required>
                    <a-input v-model="llmConfig.model" placeholder="如 gpt-4o、qwen3:8b" />
                  </a-form-item>
                </a-col>
              </a-row>
              <a-row :gutter="16">
                <a-col :span="12">
                  <a-form-item label="API Key 变量引用" required>
                    <a-input v-model="llmConfig.api_key_ref" placeholder="引用 Secret 变量的 key，如 OPENAI_API_KEY" />
                  </a-form-item>
                </a-col>
                <a-col :span="12">
                  <a-form-item label="输出格式">
                    <a-select v-model="llmConfig.response_format" allow-clear placeholder="默认文本">
                      <a-option value="Text">文本 (Text)</a-option>
                      <a-option value="JsonObject">JSON 对象 (JsonObject)</a-option>
                    </a-select>
                  </a-form-item>
                </a-col>
              </a-row>

              <a-divider>Prompt 配置</a-divider>
              <a-alert type="info" style="margin-bottom: 16px">
                Prompt 支持 <code>{<!-- -->{变量}}</code> 模板插值，可引用上下文变量和前序节点输出。
              </a-alert>
              <a-form-item label="System Prompt (系统提示词)">
                <a-textarea v-model="llmConfig.system_prompt" :auto-size="{ minRows: 2, maxRows: 8 }" placeholder="可选。定义 AI 的角色和行为约束" />
              </a-form-item>
              <a-form-item label="User Prompt (用户提示词)" required>
                <a-textarea v-model="llmConfig.user_prompt" :auto-size="{ minRows: 3, maxRows: 12 }" placeholder="必填。实际任务指令" />
              </a-form-item>

              <a-divider>用户表单定义</a-divider>
              <a-alert type="info" style="margin-bottom: 16px">
                此处定义的字段将在独立执行时展示给用户填写，值可通过 <code>{<!-- -->{key}}</code> 在 System Prompt / User Prompt 中引用。
              </a-alert>
              <a-form-item label="Form (用户输入字段)">
                <div class="form-list">
                  <div v-for="(f, idx) in llmConfig.form" :key="idx" class="form-row">
                    <a-input v-model="f.key" placeholder="字段名 (key)" style="width: 160px" />
                    <a-input v-model="f.value" placeholder="默认值 (可选)" style="flex: 1" />
                    <a-select v-model="f.type" style="width: 120px">
                      <a-option value="String">String</a-option>
                      <a-option value="Number">Number</a-option>
                      <a-option value="Bool">Bool</a-option>
                      <a-option value="Json">Json</a-option>
                    </a-select>
                    <a-input v-model="f.description" placeholder="字段说明" style="width: 200px" />
                    <a-button status="danger" @click="llmConfig.form.splice(idx, 1)">
                      <template #icon><icon-delete /></template>
                    </a-button>
                  </div>
                  <a-button type="dashed" long @click="llmConfig.form.push({ key: '', value: '', type: 'String', description: '' })">+ 添加表单字段</a-button>
                </div>
              </a-form-item>

              <a-divider>模型参数</a-divider>
              <a-row :gutter="16">
                <a-col :span="8">
                  <a-form-item label="Temperature">
                    <a-input-number v-model="llmConfig.temperature" :min="0" :max="2" :step="0.1" placeholder="默认由模型决定" />
                  </a-form-item>
                </a-col>
                <a-col :span="8">
                  <a-form-item label="最大输出 Token">
                    <a-input-number v-model="llmConfig.max_tokens" :min="1" placeholder="默认由模型决定" />
                  </a-form-item>
                </a-col>
              </a-row>

              <a-divider>运行参数</a-divider>
              <a-row :gutter="16">
                <a-col :span="8">
                  <a-form-item label="超时 (秒)">
                    <a-input-number v-model="llmConfig.timeout" :min="1" :max="600" />
                  </a-form-item>
                </a-col>
                <a-col :span="8">
                  <a-form-item label="重试次数">
                    <a-input-number v-model="llmConfig.retry_count" :min="0" :max="10" />
                  </a-form-item>
                </a-col>
                <a-col :span="8">
                  <a-form-item label="重试延迟 (秒)">
                    <a-input-number v-model="llmConfig.retry_delay" :min="0" />
                  </a-form-item>
                </a-col>
              </a-row>
            </template>

            <!-- ==================== 审批配置 ==================== -->
            <template v-else-if="form.task_type === 'Approval'">
              <a-divider>审批配置</a-divider>
              <a-row :gutter="16">
                <a-col :span="16">
                  <a-form-item label="审批标题" required>
                    <a-input v-model="approvalConfig.title" placeholder="审批标题，支持 {{变量}} 模板" />
                  </a-form-item>
                </a-col>
                <a-col :span="8">
                  <a-form-item label="审批模式" required>
                    <a-select v-model="approvalConfig.approval_mode">
                      <a-option value="Any">抢单模式 (Any)</a-option>
                      <a-option value="All">会签模式 (All)</a-option>
                      <a-option value="Majority">投票模式 (Majority)</a-option>
                    </a-select>
                  </a-form-item>
                </a-col>
              </a-row>
              <a-form-item label="审批说明">
                <a-textarea v-model="approvalConfig.description" :auto-size="{ minRows: 2 }" placeholder="可选" />
              </a-form-item>
              <a-form-item label="审批人规则">
                <div class="form-list">
                  <div v-for="(rule, idx) in approvalConfig.approvers" :key="idx" class="form-row">
                    <a-select v-model="rule.type" style="width: 160px">
                      <a-option value="User">指定用户</a-option>
                      <a-option value="Role">指定角色</a-option>
                      <a-option value="ContextVariable">上下文变量</a-option>
                    </a-select>
                    <a-input v-if="rule.type === 'User'" v-model="rule.value" placeholder="user_id" style="flex: 1" />
                    <a-select v-else-if="rule.type === 'Role'" v-model="rule.value" style="flex: 1">
                      <a-option value="TenantAdmin">TenantAdmin</a-option>
                      <a-option value="Developer">Developer</a-option>
                      <a-option value="Operator">Operator</a-option>
                    </a-select>
                    <a-input v-else v-model="rule.value" placeholder="变量名" style="flex: 1" />
                    <a-button status="danger" @click="approvalConfig.approvers.splice(idx, 1)">
                      <template #icon><icon-delete /></template>
                    </a-button>
                  </div>
                  <a-button type="dashed" long @click="approvalConfig.approvers.push({ type: 'User', value: '' })">
                    + 添加审批人规则
                  </a-button>
                </div>
              </a-form-item>
              <a-row :gutter="16">
                <a-col :span="8">
                  <a-form-item label="自审批策略">
                    <a-tooltip content="跳过发起人：当工作流发起人在审批人列表中时，自动将其移除">
                      <a-select v-model="approvalConfig.self_approval">
                        <a-option value="Skip">跳过发起人</a-option>
                        <a-option value="Allow">允许自审批</a-option>
                      </a-select>
                    </a-tooltip>
                  </a-form-item>
                </a-col>
                <a-col :span="8">
                  <a-form-item label="超时 (秒)">
                    <a-input-number v-model="approvalConfig.timeout" :min="0" placeholder="不填则不超时" />
                  </a-form-item>
                </a-col>
              </a-row>
            </template>

            <!-- ==================== gRPC 预留 ==================== -->
            <template v-else-if="form.task_type === 'Grpc'">
              <a-divider>gRPC 配置</a-divider>
              <a-alert type="info">gRPC 类型配置即将支持</a-alert>
            </template>

            <a-form-item>
              <a-space>
                <a-button type="primary" html-type="submit" :loading="saving">保存</a-button>
                <a-button @click="$router.push('/tasks')">取消</a-button>
              </a-space>
            </a-form-item>
          </a-form>
        </a-card>
      </a-tab-pane>

      <a-tab-pane v-if="isEdit" key="instances" title="实例列表">
        <a-card>
          <a-form :model="instanceFilters" layout="inline" class="filter-form">
            <a-form-item label="状态">
              <a-select v-model="instanceFilters.status" allow-clear placeholder="全部" style="width: 140px">
                <a-option value="Pending">Pending</a-option>
                <a-option value="Running">Running</a-option>
                <a-option value="Completed">Completed</a-option>
                <a-option value="Failed">Failed</a-option>
                <a-option value="Canceled">Canceled</a-option>
                <a-option value="Skipped">Skipped</a-option>
              </a-select>
            </a-form-item>
            <a-form-item>
              <a-space>
                <a-button type="primary" @click="onSearchInstances">查询</a-button>
                <a-button @click="onResetInstanceFilters">重置</a-button>
              </a-space>
            </a-form-item>
          </a-form>

          <a-table
            :data="taskInstances"
            :columns="instanceColumns"
            :loading="instancesLoading"
            row-key="id"
            :pagination="instancePagination"
            @page-change="onInstancePageChange"
            @page-size-change="onInstancePageSizeChange"
          >
            <template #task_status="{ record }">
              <status-tag :status="record.task_status" :map="TASK_INSTANCE_STATUS_MAP" />
            </template>
            <template #duration="{ record }">{{ formatDuration(record.execution_duration) }}</template>
            <template #created_at="{ record }">{{ formatDate(record.created_at) }}</template>
            <template #action="{ record }">
              <a-button type="text" size="small" @click="$router.push(`/tasks/instances/${record.task_instance_id}`)">详情</a-button>
            </template>
          </a-table>
        </a-card>
      </a-tab-pane>

      <a-tab-pane v-if="isEdit && isStandaloneType" key="execute" title="执行">
        <a-card title="独立执行">
          <a-alert type="info" style="margin-bottom: 16px">
            填写参数后点击「创建并执行」，系统将创建一个任务实例并立即投递执行。
          </a-alert>

          <a-form layout="vertical" size="small">
            <template v-if="execFormDefs.length > 0">
              <a-form-item
                v-for="f in execFormDefs"
                :key="f.key"
                :label="f.key"
                :extra="f.description || undefined"
              >
                <a-input
                  v-if="f.type === 'String' || f.type === 'Variable'"
                  v-model="execFormValues[f.key]"
                  :placeholder="f.defaultValue != null ? String(f.defaultValue) : ''"
                />
                <a-input-number v-else-if="f.type === 'Number'" v-model="execFormValues[f.key]" style="width: 100%" />
                <a-select v-else-if="f.type === 'Bool'" v-model="execFormValues[f.key]">
                  <a-option :value="true">true</a-option>
                  <a-option :value="false">false</a-option>
                </a-select>
                <a-textarea v-else v-model="execFormValues[f.key]" :auto-size="{ minRows: 2 }" placeholder="JSON" />
              </a-form-item>
            </template>

            <a-collapse :default-active-key="[]" style="margin-bottom: 16px">
              <a-collapse-item key="raw" header="高级：原始 Context (JSON)">
                <a-textarea v-model="execRawContext" :auto-size="{ minRows: 3, maxRows: 10 }" placeholder='{"key": "value"}' />
              </a-collapse-item>
            </a-collapse>

            <a-space>
              <a-button type="primary" :loading="executing" @click="handleCreateAndExecute">创建并执行</a-button>
            </a-space>
          </a-form>

          <template v-if="execResultInstance">
            <a-divider />
            <a-descriptions :column="2" size="small" bordered>
              <a-descriptions-item label="实例ID">{{ execResultInstance.task_instance_id }}</a-descriptions-item>
              <a-descriptions-item label="状态">
                <status-tag :status="execResultInstance.task_status" :map="TASK_INSTANCE_STATUS_MAP" />
              </a-descriptions-item>
            </a-descriptions>
            <a-space style="margin-top: 12px">
              <a-button type="text" @click="$router.push(`/tasks/instances/${execResultInstance.task_instance_id}`)">查看详情</a-button>
            </a-space>
          </template>
        </a-card>
      </a-tab-pane>
    </a-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { taskApi } from '../../../api/task'
import { taskInstanceApi } from '../../../api/task-instance'
import { TASK_INSTANCE_STATUS_MAP } from '../../../utils/constants'
import { formatDate, formatDuration } from '../../../utils/format'
import StatusTag from '../../../components/common/status-tag.vue'
import { Notification } from '@arco-design/web-vue'
import { IconDelete } from '@arco-design/web-vue/es/icon'
import type { TaskEntity, TaskInstanceEntity, TaskInstanceStatus, HttpMethod, FormField, FormValueType, ListTaskInstancesParams } from '../../../types/task'

const route = useRoute()
const router = useRouter()
const isEdit = computed(() => !!route.params.id)
const taskId = computed(() => route.params.id as string)
const initialTab = computed(() => {
  const tab = route.query.tab as string
  return (tab === 'execute' || tab === 'instances') ? tab : 'config'
})
const pageLoading = ref(false)
const saving = ref(false)

const form = reactive({
  name: '',
  task_type: 'Http' as string,
  description: '',
  status: 'Draft' as string,
})

interface FormRow {
  key: string
  value: any
  type: FormValueType
  description: string
}

const httpConfig = reactive({
  url: '',
  method: 'Get' as HttpMethod,
  headers: [] as FormRow[],
  body: [] as FormRow[],
  form: [] as FormRow[],
  retry_count: 0,
  retry_delay: 0,
  timeout: 30,
  success_condition: null as string | null,
})

const llmConfig = reactive({
  base_url: 'https://api.openai.com/v1',
  model: '',
  api_key_ref: '',
  system_prompt: '' as string | null,
  user_prompt: '',
  temperature: null as number | null,
  max_tokens: null as number | null,
  timeout: 60,
  retry_count: 0,
  retry_delay: 3,
  response_format: null as string | null,
  form: [] as FormRow[],
})

const approvalConfig = reactive({
  title: '',
  description: '' as string,
  approval_mode: 'Any' as string,
  approvers: [] as { type: string; value: string }[],
  self_approval: 'Skip' as string,
  timeout: null as number | null,
})

// ---- Header 预设与建议 ----

const headerPresets = [
  { key: 'Content-Type', value: 'application/json', type: 'String' as FormValueType },
  { key: 'Authorization', value: 'Bearer ', type: 'Variable' as FormValueType },
  { key: 'Accept', value: 'application/json', type: 'String' as FormValueType },
  { key: 'X-API-Key', value: '', type: 'Variable' as FormValueType },
]

const headerSuggestions = [
  'Content-Type', 'Authorization', 'Accept', 'X-API-Key',
  'Cache-Control', 'User-Agent', 'X-Request-ID', 'X-Forwarded-For',
  'If-None-Match', 'Origin', 'Referer',
]

function addPresetHeader(preset: { key: string; value: string; type: FormValueType }) {
  const exists = httpConfig.headers.some(h => h.key === preset.key)
  if (exists) {
    Notification.warning({ content: `Header "${preset.key}" 已存在` })
    return
  }
  httpConfig.headers.push({ key: preset.key, value: preset.value, type: preset.type, description: '' })
}

function addHeaderRow() {
  httpConfig.headers.push({ key: '', value: '', type: 'String', description: '' })
}

function addBodyRow() {
  httpConfig.body.push({ key: '', value: '', type: 'String', description: '' })
}

function addFormRow() {
  httpConfig.form.push({ key: '', value: '', type: 'String', description: '' })
}

// ---- 构建提交数据 ----

function toFormFields(rows: FormRow[]): FormField[] {
  return rows
    .filter(r => r.key.trim() !== '')
    .map(r => {
      const field: FormField = { key: r.key, value: r.value, type: r.type }
      if (r.description) field.description = r.description
      return field
    })
}

function buildTaskTemplate() {
  if (form.task_type === 'Http') {
    return {
      Http: {
        url: httpConfig.url,
        method: httpConfig.method,
        headers: toFormFields(httpConfig.headers),
        body: toFormFields(httpConfig.body),
        form: toFormFields(httpConfig.form),
        retry_count: httpConfig.retry_count,
        retry_delay: httpConfig.retry_delay,
        timeout: httpConfig.timeout,
        success_condition: httpConfig.success_condition || null,
      },
    }
  }
  if (form.task_type === 'Llm') {
    return {
      Llm: {
        base_url: llmConfig.base_url,
        model: llmConfig.model,
        api_key_ref: llmConfig.api_key_ref,
        system_prompt: llmConfig.system_prompt || null,
        user_prompt: llmConfig.user_prompt,
        temperature: llmConfig.temperature,
        max_tokens: llmConfig.max_tokens,
        timeout: llmConfig.timeout,
        retry_count: llmConfig.retry_count,
        retry_delay: llmConfig.retry_delay,
        response_format: llmConfig.response_format || null,
        form: toFormFields(llmConfig.form),
      },
    }
  }
  if (form.task_type === 'Approval') {
    return {
      Approval: {
        name: form.name,
        title: approvalConfig.title,
        description: approvalConfig.description || null,
        approvers: approvalConfig.approvers.map(r => {
          if (r.type === 'User') return { User: r.value }
          if (r.type === 'Role') return { Role: r.value }
          return { ContextVariable: r.value }
        }),
        approval_mode: approvalConfig.approval_mode,
        timeout: approvalConfig.timeout || null,
        self_approval: approvalConfig.self_approval,
      },
    }
  }
  return form.task_type
}

async function handleSave() {
  saving.value = true
  try {
    const payload: Partial<TaskEntity> = {
      name: form.name,
      task_type: form.task_type as any,
      description: form.description,
      status: form.status as any,
      task_template: buildTaskTemplate() as any,
    }
    if (isEdit.value) {
      await taskApi.update(route.params.id as string, payload)
    } else {
      await taskApi.create(payload)
    }
    Notification.success({ content: '保存成功' })
    router.push('/tasks')
  } catch {} finally { saving.value = false }
}

// ---- 编辑模式加载 ----

function formFieldsToRows(fields: FormField[] | undefined | null): FormRow[] {
  if (!fields || !Array.isArray(fields)) return []
  return fields.map(f => ({
    key: f.key,
    value: f.value,
    type: f.type || 'String',
    description: f.description || '',
  }))
}

// ---- 实例列表 ----

const taskInstances = ref<TaskInstanceEntity[]>([])
const instancesLoading = ref(false)
const instancesLoaded = ref(false)

const instanceFilters = reactive<{
  status: TaskInstanceStatus | undefined
}>({
  status: undefined,
})

const instancePagination = reactive({
  current: 1,
  pageSize: 10,
  total: 0,
  showTotal: true,
  showPageSize: true,
  pageSizeOptions: [10, 20, 50, 100],
})

const instanceColumns = [
  { title: '实例ID', dataIndex: 'task_instance_id', ellipsis: true, width: 200 },
  { title: '状态', slotName: 'task_status', width: 100 },
  { title: '耗时', slotName: 'duration', width: 100 },
  { title: '创建时间', slotName: 'created_at', width: 180 },
  { title: '操作', slotName: 'action', width: 100 },
]

function buildInstanceParams(): ListTaskInstancesParams {
  const p: ListTaskInstancesParams = {
    task_id: taskId.value,
    page: instancePagination.current,
    page_size: instancePagination.pageSize,
  }
  if (instanceFilters.status) p.status = instanceFilters.status
  return p
}

async function fetchInstances() {
  instancesLoading.value = true
  try {
    const res = await taskInstanceApi.list(buildInstanceParams())
    taskInstances.value = res.data.items
    instancePagination.total = Number(res.data.total)
  } catch {} finally { instancesLoading.value = false }
}

function onInstancePageChange(page: number) {
  instancePagination.current = page
  fetchInstances()
}

function onInstancePageSizeChange(size: number) {
  instancePagination.pageSize = size
  instancePagination.current = 1
  fetchInstances()
}

function onSearchInstances() {
  instancePagination.current = 1
  fetchInstances()
}

function onResetInstanceFilters() {
  instanceFilters.status = undefined
  instancePagination.current = 1
  fetchInstances()
}

function handleTabChange(key: string) {
  if (key === 'instances' && !instancesLoaded.value) {
    instancesLoaded.value = true
    fetchInstances()
  }
}

// ---- 执行 Tab ----

const STANDALONE_TYPES = ['Http', 'Llm']
const isStandaloneType = computed(() => STANDALONE_TYPES.includes(form.task_type))

interface ExecFormDef {
  key: string
  type: string
  defaultValue: any
  description: string
}

const execFormDefs = computed<ExecFormDef[]>(() => {
  if (form.task_type === 'Http') {
    return httpConfig.form
      .filter(f => f.key.trim())
      .map(f => ({ key: f.key, type: f.type, defaultValue: f.value, description: f.description }))
  }
  if (form.task_type === 'Llm') {
    return llmConfig.form
      .filter(f => f.key.trim())
      .map(f => ({ key: f.key, type: f.type, defaultValue: f.value, description: f.description }))
  }
  return []
})

const execFormValues = reactive<Record<string, any>>({})
const execRawContext = ref('')
const executing = ref(false)
const execResultInstance = ref<TaskInstanceEntity | null>(null)

function buildExecContext(): Record<string, any> {
  const ctx: Record<string, any> = {}
  let raw: Record<string, any> = {}
  if (execRawContext.value.trim()) {
    try { raw = JSON.parse(execRawContext.value) } catch {}
  }
  Object.assign(ctx, raw)
  for (const def of execFormDefs.value) {
    const val = execFormValues[def.key]
    if (val !== undefined && val !== null && val !== '') {
      ctx[def.key] = val
    } else if (def.defaultValue !== undefined && def.defaultValue !== null && def.defaultValue !== '') {
      ctx[def.key] = def.defaultValue
    }
  }
  return ctx
}

async function handleCreateAndExecute() {
  executing.value = true
  execResultInstance.value = null
  try {
    const context = buildExecContext()
    const createRes = await taskInstanceApi.create({ task_id: taskId.value, context })
    const instanceId = createRes.data.task_instance_id
    await taskInstanceApi.execute(instanceId)
    execResultInstance.value = createRes.data
    Notification.success({ content: '已创建并提交执行' })
    if (instancesLoaded.value) fetchInstances()
  } catch {
    Notification.error({ content: '执行失败' })
  } finally {
    executing.value = false
  }
}

// ---- 初始化 ----

onMounted(async () => {
  if (isEdit.value) {
    pageLoading.value = true
    try {
      const res = await taskApi.get(route.params.id as string)
      const entity = res.data
      form.name = entity.name
      form.task_type = entity.task_type
      form.description = entity.description
      form.status = entity.status
      if (entity.task_type === 'Http' && typeof entity.task_template === 'object' && 'Http' in entity.task_template) {
        const tpl = entity.task_template.Http
        httpConfig.url = tpl.url
        httpConfig.method = tpl.method
        httpConfig.headers = formFieldsToRows(tpl.headers)
        httpConfig.body = formFieldsToRows(tpl.body)
        httpConfig.form = formFieldsToRows(tpl.form)
        httpConfig.retry_count = tpl.retry_count
        httpConfig.retry_delay = tpl.retry_delay
        httpConfig.timeout = tpl.timeout
        httpConfig.success_condition = tpl.success_condition
      } else if (entity.task_type === 'Llm' && typeof entity.task_template === 'object' && 'Llm' in entity.task_template) {
        const tpl = entity.task_template.Llm
        llmConfig.base_url = tpl.base_url
        llmConfig.model = tpl.model
        llmConfig.api_key_ref = tpl.api_key_ref
        llmConfig.system_prompt = tpl.system_prompt
        llmConfig.user_prompt = tpl.user_prompt
        llmConfig.temperature = tpl.temperature
        llmConfig.max_tokens = tpl.max_tokens
        llmConfig.timeout = tpl.timeout
        llmConfig.retry_count = tpl.retry_count
        llmConfig.retry_delay = tpl.retry_delay
        llmConfig.response_format = tpl.response_format
        llmConfig.form = formFieldsToRows(tpl.form)
      } else if (entity.task_type === 'Approval' && typeof entity.task_template === 'object' && 'Approval' in entity.task_template) {
        const tpl = entity.task_template.Approval
        approvalConfig.title = tpl.title
        approvalConfig.description = tpl.description || ''
        approvalConfig.approval_mode = tpl.approval_mode
        approvalConfig.timeout = tpl.timeout
        approvalConfig.self_approval = tpl.self_approval || 'Skip'
        approvalConfig.approvers = tpl.approvers.map((r: any) => {
          if (r.User !== undefined) return { type: 'User', value: r.User }
          if (r.Role !== undefined) return { type: 'Role', value: r.Role }
          if (r.ContextVariable !== undefined) return { type: 'ContextVariable', value: r.ContextVariable }
          return { type: 'User', value: '' }
        })
      }
    } catch {} finally { pageLoading.value = false }
  }
})
</script>

<style scoped>
.form-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  width: 100%;
}

.form-row {
  display: flex;
  gap: 8px;
  align-items: flex-start;
}

.preset-tags {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}

.preset-label {
  font-size: 13px;
  color: var(--color-text-3);
}

.preset-tag {
  cursor: pointer;
}

.filter-form {
  margin-bottom: 16px;
}
</style>
