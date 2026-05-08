# Bugfix Plan: Retry 缺少 Start 调度 + Skip 后 Task Instance 状态不同步

---

## Bug 1：Retry Handler 缺少 Dispatch Start

### 现象

用户在工作流实例 `Failed` 时点击"重试"按钮，工作流状态变为 `Pending`，但**不会自动开始执行**，必须再手动点"执行"。与用户预期不一致（"重试"应该等价于"重置+自动启动"）。

### 根因

`retry_instance` handler 只做了状态转换和节点重置，**没有 dispatch `WorkflowEvent::Start`**：

```rust
// src/crates/api/src/handler/workflow/workflow_instance_handler.rs:158-167
async fn retry_instance(...) -> ... {
    handler.instance_service.get_workflow_instance_scoped(&auth.tenant_id, &id).await?;
    let result = handler.instance_service.retry_instance(&id).await?;
    info!(workflow_instance_id = %id, "workflow instance retried");
    Ok(Json(Response::success(result)))
    // ← 缺少 dispatch_workflow(Start) 调用
}
```

对比 `skip_node` handler（第 211-225 行），它在状态变更后会 dispatch `NodeCallback`。`retry_instance` 应有类似的调度逻辑。

### `retry_instance` 服务层做了什么

```rust
// src/crates/domain/src/workflow/service.rs:301-323
pub async fn retry_instance(&self, workflow_instance_id: &str) -> ... {
    // 1. 状态转换: Failed → Pending
    let mut instance = self.transfer_status(
        workflow_instance_id,
        &WorkflowInstanceStatus::Failed,
        &WorkflowInstanceStatus::Pending,
    ).await?;

    // 2. 重置当前失败节点
    let current_node_id = instance.get_current_node();
    if let Some(node) = instance.nodes.iter_mut().find(|n| n.node_id == current_node_id) {
        if node.status == NodeExecutionStatus::Failed {
            node.status = NodeExecutionStatus::Pending;
            node.error_message = None;
            node.task_instance.output = None;      // ← 清空容器 output/state
            node.task_instance.error_message = None;
        }
    }

    self.repository.save_workflow_instance(&instance).await?;
    Ok(instance)
}
```

服务层正确：状态 `Failed → Pending`，重置当前节点。但 handler 层没有后续动作。

### 解决方案

在 `retry_instance` handler 中，服务调用成功后 dispatch `WorkflowEvent::Start`：

```rust
async fn retry_instance(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    handler.instance_service.get_workflow_instance_scoped(&auth.tenant_id, &id).await?;
    let result = handler.instance_service.retry_instance(&id).await?;

    // 新增: dispatch Start 让工作流从重置的节点继续执行
    handler
        .dispatcher
        .dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: id.clone(),
            tenant_id: auth.tenant_id.clone(),
            event: WorkflowEvent::Start,
        })
        .await
        .map_err(|e| {
            error!(workflow_instance_id = %id, error = %e, "failed to dispatch Start after retry");
            ApiError::internal(e.to_string())
        })?;

    info!(workflow_instance_id = %id, "workflow instance retried and Start dispatched");
    Ok(Json(Response::success(result)))
}
```

### 影响范围

- 只修改 `workflow_instance_handler.rs` 的 `retry_instance` 函数
- 不影响服务层逻辑
- 不影响其他 handler

### 注意事项

1. **容器节点重试时 output 被清空**：`retry_instance` 中 `node.task_instance.output = None` 会清空 Parallel/ForkJoin 的整个 state（包括 `processed_callbacks`、`results`、计数器）。这意味着重试后 Parallel 会完全重新执行，重新 dispatch 所有子任务。这是正确行为——"重试"就是从头来过。

2. **子任务 ID 冲突**：重试后 Parallel 重新 dispatch 的子任务 ID 格式为 `{wf_id}-{node_id}-{index}`，与上一轮执行的 ID 相同。`ensure_task_instance_for_job` 会检测到已存在的 task_instance 并更新（upsert 语义），不会冲突。

3. **Sweeper 兜底**：即使 dispatch Start 失败，Sweeper 会检测到 `Pending` 状态的实例并尝试启动。但依赖 Sweeper 的延迟（默认 30 秒周期）会导致用户感知到卡顿。因此在 handler 层主动 dispatch 是更好的做法。

---

## Bug 2：Skip 后独立 Task Instance 状态不同步

### 现象

1. **普通节点 skip**：工作流实例中嵌入的 `task_instance.task_status` 变为 `Completed`，但 `task_instances` 集合中的独立文档仍然是 `Failed`
2. **容器子任务 skip**：容器的 output state 中子任务状态变为 `Skipped`，但对应的独立 task_instance 文档仍然是 `Failed`

这会导致：
- 用户在任务实例页面看到已 skip 的任务仍显示 `Failed`，可以点"重试"
- Sweeper 扫描 task_instances 集合时，读到的仍是 `Failed` 状态，可能补发错误的回调

### 根因

`skip_workflow_node` 服务方法只修改了 `workflow_instances` 集合中嵌入的 task_instance 数据，**没有更新 `task_instances` 集合中的独立文档**：

```rust
// src/crates/domain/src/workflow/service.rs (普通节点 skip)
inst.nodes[idx].status = NodeExecutionStatus::Skipped;
inst.nodes[idx].task_instance.output = Some(output);
inst.nodes[idx].task_instance.task_status = TaskInstanceStatus::Completed;
// ← 只更新了嵌入副本，没有更新独立的 task_instances 文档
```

对于容器子任务 skip，更新是通过 `NodeCallback` → `handle_callback` 路径完成的，同样不涉及独立的 task_instance 文档。

`WorkflowInstanceService` 当前没有注入 `TaskInstanceService`，因此无法直接更新独立文档。

### 解决方案

#### 方案选择

| 方案 | 描述 | 优点 | 缺点 |
|------|------|------|------|
| A. 服务层注入 | 在 `WorkflowInstanceService` 中注入 `TaskInstanceService` | 原子性好，一处修改 | 增加了服务间耦合 |
| B. Handler 层协调 | 在 `skip_node` handler 中分别调用两个 service | 不增加服务层耦合 | handler 逻辑变复杂 |
| C. 事件驱动 | skip 成功后发一个内部事件，由 listener 异步更新 | 完全解耦 | 实现复杂度高，最终一致性 |

**推荐方案 A**：在 `WorkflowInstanceService` 中注入 `TaskInstanceService`，在 skip 操作中同步更新独立的 task_instance 文档。理由：skip 是一个完整的业务动作，涉及两个集合的数据一致性，应在 service 层保证。

#### 实现细节

**1. 注入 TaskInstanceService**

```rust
// src/crates/domain/src/workflow/service.rs
pub struct WorkflowInstanceService {
    repository: Arc<dyn WorkflowInstanceRepository>,
    task_instance_svc: Arc<TaskInstanceService>,  // 新增
}

impl WorkflowInstanceService {
    pub fn new(
        repository: Arc<dyn WorkflowInstanceRepository>,
        task_instance_svc: Arc<TaskInstanceService>,  // 新增
    ) -> Self {
        Self { repository, task_instance_svc }
    }
}
```

**2. 普通节点 skip 时同步更新**

在 `skip_workflow_node` 的普通节点分支中，获取并更新独立的 task_instance：

```rust
// 普通节点 skip (在设置嵌入副本状态后)
let task_instance_id = inst.nodes[idx].task_instance.task_instance_id.clone();
if let Ok(mut task_inst) = self.task_instance_svc
    .get_task_instance_entity(task_instance_id.clone()).await
{
    task_inst.task_status = TaskInstanceStatus::Completed;
    task_inst.output = Some(output.clone());
    task_inst.error_message = None;
    let _ = self.task_instance_svc
        .update_task_instance_entity(task_inst).await;
}
```

**3. 容器子任务 skip 时同步更新**

在 `skip_workflow_node` 的容器分支中（`Failed → Await` 转换后），更新 child_task_id 对应的独立 task_instance：

```rust
// 容器子任务 skip (在 Failed → Await 转换后)
if let Ok(mut child_task) = self.task_instance_svc
    .get_task_instance_entity(cid.to_string()).await
{
    child_task.task_status = TaskInstanceStatus::Completed;
    child_task.output = Some(output.clone());
    child_task.error_message = None;
    let _ = self.task_instance_svc
        .update_task_instance_entity(child_task).await;
}
```

**4. 更新依赖注入链**

需要在所有构造 `WorkflowInstanceService` 的地方传入 `TaskInstanceService`：
- `src/bin/engine.rs` 中的 service 构建
- `src/crates/api/src/...` 中的 handler 构建
- 可能涉及 `PluginManager` 中的构建

### 影响范围

- `workflow/service.rs`：修改 `WorkflowInstanceService` 结构体和 `skip_workflow_node` 方法
- `engine.rs`：更新 service 构建
- API handler 构建处：传入 `TaskInstanceService`
- **不影响** 任务执行逻辑、插件逻辑、前端

### 注意事项

1. **TaskInstanceStatus::Skipped 是否需要**：当前使用 `Completed` 标记被 skip 的任务。如果未来需要区分"正常完成"和"被跳过"，可以新增 `TaskInstanceStatus::Skipped` 变体。当前阶段用 `Completed` 足够——关键是让它不再显示为 `Failed`。

2. **Sweeper 补发行为**：修复后，被 skip 的子任务在 task_instances 集合中变为 `Completed`。Sweeper 的 `recover_await_container` 扫到 `Completed` 的子任务时会补发 `Success` 回调，这与 skip 后的语义一致（Skipped 计为 Success）。

3. **更新失败容错**：独立 task_instance 的更新用 `let _ =` 忽略错误，因为这是"尽力同步"而非事务性保证。即使更新失败，工作流的主状态机已正确推进。

---

## 实施优先级

| 优先级 | Bug | 修复复杂度 | 影响 |
|--------|-----|-----------|------|
| P0 | Retry 缺少 Start | 低（3 行代码） | 用户体验：重试后不自动执行 |
| P1 | Task Instance 不同步 | 中（注入 + 多处更新） | 数据一致性、Sweeper 正确性 |
