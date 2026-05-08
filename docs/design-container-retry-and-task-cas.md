# 设计方案：容器子任务原子重试 + Task Worker CAS 状态机

---

## 1. 设计目标

1. **容器子任务原子重试**：与 skip 对称，只能重试单个容器内的失败子任务，不重新执行已 Skipped/Success 的子任务
2. **Task Worker CAS 状态机保护**：通过 MongoDB 原子 CAS 操作保证只有一个 Worker 执行一个 task_instance，终态不可重入
3. **禁止工作流级别粗暴重试容器节点**：`retry_instance` 对容器节点不再清空 output 重新执行全部子任务

---

## 2. 当前问题

### 2.1 `retry_instance` 无脑清空容器 output

```rust
// retry_instance 当前行为
node.status = NodeExecutionStatus::Pending;
node.task_instance.output = None;  // ← 擦掉 results、processed_callbacks、skipped_count 全部
```

导致 Parallel.execute() 从头 dispatch 所有子任务，包括已 Skipped 的。

### 2.2 Task Worker 无状态机保护

Task Worker 从 Redis 队列取到 job 后直接执行，不走 `transfer_status` CAS。多个 Worker 可以并发执行同一个 task_instance，终态任务也可以被重新执行。

### 2.3 `ensure_task_instance_for_job` 不检查终态

task_instance 已存在时只检查 task_type，不检查 status。不阻止对终态 task 的重新 dispatch。

---

## 3. 总体方案

### 3.1 统一 retry-node API（与 skip-node 对称）

与 `skip-node` 采用相同的设计模式：**一个统一的 endpoint**，通过 `child_task_id: Option<String>` 区分原子节点重试和容器子任务重试。

#### API

```
POST /workflow/instance/{id}/retry-node
Body: {
  "node_id": "node_6",
  "child_task_id": "xxx-node_6-1"   // 可选，容器节点时必填
}
```

- `child_task_id = None` → 原子节点重试（等价于当前 `retry_instance` 对非容器节点的行为）
- `child_task_id = Some(id)` → 容器子任务重试（只重试该子任务）

#### 服务层：`retry_workflow_node`

```
retry_workflow_node(tenant_id, workflow_instance_id, node_id, child_task_id):

  共同校验：
  - workflow status ∈ {Failed, Await}
  - node_id == current_node

  分支 A：容器子任务重试（child_task_id 有值）
  ──────────────────────────────────────────────
  1. 校验
     - node_type ∈ {Parallel, ForkJoin}
     - child_task_id 格式正确（属于该容器）
     - results[child_task_id].status == "Failed"（只允许重试 Failed 的）

  2. 更新容器 output state
     - 从 processed_callbacks 中移除 child_task_id
     - failed_count -= 1
     - results[child_task_id] = null（标记待重新 dispatch）

  3. 重置独立 task_instance
     - task_instance_svc.transfer_status(child_task_id, Failed, Pending)
       → CAS 保证只从 Failed 转到 Pending
     - 清空 output, error_message

  4. 状态转换
     - 若 workflow status == Failed → 转为 Await
     - 保存 workflow instance

  5. 重新 dispatch 子任务
     - 构建 ExecuteTaskJob（带 caller_context）
     - dispatch_task()

  6. 返回更新后的 workflow instance

  分支 B：原子节点重试（child_task_id 为 None）
  ──────────────────────────────────────────────
  1. 校验
     - node_type 不能是 SubWorkflow
     - node_type 不能是容器（Parallel/ForkJoin） → 容器必须提供 child_task_id
     - node.status == Failed

  2. 重置节点
     - node.status = Pending
     - 清空 output, error_message
     - task_instance_svc.transfer_status(task_instance_id, Failed, Pending)

  3. 状态转换
     - workflow status: Failed → Pending

  4. dispatch WorkflowEvent::Start

  5. 返回更新后的 workflow instance
```

#### 回调处理（容器子任务重试）

子任务执行完成后，NodeCallback 正常到达容器 handle_callback。由于 child_task_id 已从 `processed_callbacks` 移除，不会被当作 duplicate。正常累计计数器，重新评估完成条件。

#### 前端

- 容器子任务表格中，对 status == "Failed" 的行增加"重试"按钮（与"跳过"按钮并列）
- 非容器节点的工作流级"重试"按钮保持不变，改为调用 `retry-node` API（`child_task_id = None`）

### 3.2 废弃 `retry` 接口，统一为 `retry-node`

**废弃** `POST /{id}/retry`（`retry_instance`），所有重试操作统一走 `POST /{id}/retry-node`。

- 移除 `retry_instance` handler 和路由
- 移除（或标记废弃）`WorkflowInstanceService::retry_instance` 方法
- 前端所有重试调用改为 `retryNode` API

#### 前端适配

工作流状态为 Failed 时：
- 当前节点为**非容器**：顶部显示"重试"按钮 → 调用 `retryNode({ node_id: current_node })`
- 当前节点为**容器**：顶部**不显示**"重试"按钮，引导用户在子任务表格中使用每行的"重试"/"跳过"按钮

### 3.3 Task Worker CAS 状态机保护

#### 修改 `handle_task_job` (engine.rs)

执行前通过 CAS 原子转换 `Pending → Running`：

```
handle_task_job(job):
  1. load task_instance
  2. task_instance_svc.submit_instance(id)   // CAS: Pending → Running
     ├── 成功 → 当前 worker 独占
     ├── CAS 失败 → warn("task already claimed") → return Ok(())
     └── 状态机拒绝（Completed→Running 不合法）→ warn("task in terminal state") → return Ok(())
  3. execute_task()
  4. task_instance_svc.complete_instance(id) 或 fail_instance(id)  // CAS: Running → Completed/Failed
  5. dispatch NodeCallback
```

**不再使用** `update_task_instance_entity`（无保护的 replace_one）来更新 status。所有状态变更都走 `transfer_status` CAS。

但 output、error_message、input 等非 status 字段仍需在 status 转换后通过 `update_task_instance_entity` 写入（或扩展 transfer_status 携带附加字段）。

#### 完整的 Task Worker 新流程

```rust
async fn handle_task_job(job, ctx) -> Result<()> {
    let task_svc = ctx.task_manager.task_instance_svc();

    // Step 1: CAS claim — Pending → Running
    let task_instance = match task_svc.submit_instance(&job.task_instance_id).await {
        Ok(inst) => inst,
        Err(e) => {
            warn!(task_instance_id = %job.task_instance_id, error = %e,
                "task instance not claimable, skipping");
            return Ok(());  // 静默退出，不发 callback
        }
    };

    // Step 2: Execute
    let exec_result = match task_manager.execute_task(&task_instance).await {
        Ok(r) => r,
        Err(e) => {
            // 执行异常 → 标记 Failed
            task_svc.fail_instance(&job.task_instance_id).await;
            // ... dispatch Failed callback
            return Err(e);
        }
    };

    // Step 3: CAS finalize — Running → Completed/Failed
    let final_status = match exec_result.status {
        NodeExecutionStatus::Success => {
            task_svc.complete_instance(&job.task_instance_id).await;
            TaskInstanceStatus::Completed
        }
        NodeExecutionStatus::Failed => {
            task_svc.fail_instance(&job.task_instance_id).await;
            TaskInstanceStatus::Failed
        }
        _ => { /* ... */ }
    };

    // Step 4: 写入 output/error 等字段
    let mut entity = task_svc.get_task_instance_entity(job.task_instance_id.clone()).await?;
    entity.output = exec_result.output.clone();
    entity.input = exec_result.input.clone();
    entity.error_message = exec_result.error_message.clone();
    task_svc.update_task_instance_entity(entity).await?;

    // Step 5: Dispatch callback
    if let Some(caller) = job.caller_context { /* dispatch NodeCallback */ }
    Ok(())
}
```

### 3.4 `ensure_task_instance_for_job` 终态检查

当 task_instance 已存在且处于终态（Completed/Canceled）时，应该 warn 并跳过，不再无声地 `return Ok(())`：

```rust
if let Ok(existing) = task_svc.get_task_instance_entity(job.task_instance_id.clone()).await {
    if existing.task_status.is_terminal() {
        warn!(task_instance_id = %job.task_instance_id,
            status = ?existing.task_status,
            "refusing to overwrite terminal task_instance");
        return Ok(());  // 或返回 Err 阻止 dispatch
    }
    // ... 现有的 task_type 校验逻辑
    return Ok(());
}
```

---

## 4. `TaskInstanceStatus` 状态机补充

当前状态转换规则：

```
Pending   → Running | Canceled
Running   → Completed | Failed
Failed    → Pending (retry) | Canceled
Completed → (终态)
Canceled  → (终态)
```

需要确认的点：
- `Failed → Pending` 已有 ✓（支持子任务重试）
- `Completed → *` 不允许 ✓（终态保护，Skipped 任务 status=Completed 不可被重执行）
- `Running → Running` 不允许 ✓（防止多 worker 并发 claim）

**无需修改**，现有状态机已完备。

---

## 5. 容器子任务重试的完整时序

```
用户点击 node_6-1 的"重试"按钮
  ↓
POST /workflow/instance/{id}/retry-node
  body: { node_id: "node_6", child_task_id: "xxx-node_6-1" }
  ↓
retry_workflow_node() 服务层（分支 A: 容器子任务）:
  ├── 校验 child 在 results 中 status == "Failed" ✓
  ├── 从 processed_callbacks 移除 "xxx-node_6-1"
  ├── failed_count -= 1
  ├── results["xxx-node_6-1"] = null
  ├── task_instance_svc.transfer_status("xxx-node_6-1", Failed, Pending) // CAS
  ├── 清空 task_instance 的 output/error
  ├── 若 workflow status == Failed → Await
  ├── 保存 workflow instance
  └── dispatch_task(ExecuteTaskJob { task_instance_id: "xxx-node_6-1", ... })
  ↓
Task Worker 收到 job:
  ├── submit_instance("xxx-node_6-1")  // CAS: Pending → Running
  ├── execute_task()
  ├── 成功 → complete_instance("xxx-node_6-1")  // CAS: Running → Completed
  └── dispatch NodeCallback(node_6, "xxx-node_6-1", Success, output)
  ↓
Parallel handle_callback:
  ├── "xxx-node_6-1" 不在 processed_callbacks → 正常处理
  ├── success_count += 1
  ├── results["xxx-node_6-1"] = { status: "Success", output: {...} }
  ├── 追加到 processed_callbacks
  ├── all_done? success_count(2) + failed_count(0) == total(2) ✓
  ├── failed_count(0) == 0 → Success!
  └── 工作流继续推进到下一个节点
```

---

## 6. 实施任务清单

| 步骤 | 文件 | 内容 | 优先级 |
|------|------|------|--------|
| 1 | `shared/workflow.rs` | 确认 `TaskInstanceStatus` 状态机（无需改动） | P0 |
| 2 | `workflow/service.rs` | 新增 `retry_workflow_node` 统一方法（分支 A: 容器子任务, 分支 B: 原子节点） | P0 |
| 3 | `workflow/service.rs` + handler + 路由 | 废弃 `retry_instance` 及其 handler/路由 | P0 |
| 4 | `api/handler/workflow_instance_handler.rs` | 新增 `retry_node` handler + 路由 `/{id}/retry-node` | P0 |
| 5 | `api/handler/workflow_instance_request.rs` | 新增 `RetryWorkflowNodeRequest { node_id, child_task_id? }` | P0 |
| 6 | `engine.rs` handle_task_job | CAS claim (submit_instance) + CAS finalize | P0 |
| 7 | `plugin/manager/ensure_task_job.rs` | 终态检查 | P1 |
| 8 | `frontend/detail.vue` | 子任务表格增加"重试"按钮 | P0 |
| 9 | `frontend/detail.vue` | 容器节点时隐藏顶部"重试"按钮；非容器时改调 retry-node API | P0 |
| 10 | `frontend/api/workflow.ts` | 新增 `retryNode` API 调用 | P0 |

---

## 7. 注意事项

### 7.1 Sweeper 兼容性

子任务重试后 task_instance 状态为 Pending → Running → Completed/Failed。Sweeper 的 `recover_await_container` 读取 task_instance 状态：

- `Pending` / `Running` → `redispatch_task()` — 正确（重试中的任务可能需要恢复）
- `Completed` → `supplement_callback(Success)` — 正确
- `Failed` → `supplement_callback(Failed)` — 正确

无需修改 Sweeper 逻辑。

### 7.2 子任务重试时的 input 重新解析

重试的子任务需要使用当前的 context 重新解析 HTTP 模板。`ensure_task_instance_for_job` 会处理这个（如果 task_instance 已存在则跳过创建，但 input 需要重新生成）。

**需要额外处理**：重试时需清空旧的 `task_instance.input`，让 task worker 或 ensure 重新解析。可以在 `retry_container_child` 中清空 input。

### 7.3 与已实现的 skip 覆盖机制的关系

- **Skip**：通过 NodeCallback(Skipped) 覆盖 processed_callbacks 中的 Failed 条目（走 handle_callback 的 Skipped 覆盖路径）
- **Retry**：通过从 processed_callbacks 中**移除** Failed 条目，重新 dispatch 子任务，让新回调作为**首次回调**正常处理

两者互不冲突，Skip 是"跳过+标记完成"，Retry 是"重新执行"。

### 7.4 并发安全

- `retry_container_child` 更新 workflow instance 时通过乐观锁（epoch）保护
- task_instance 的 `Failed → Pending` 通过 CAS 保护
- Task Worker 的 `Pending → Running` 通过 CAS 保护
- 多层 CAS 确保即使 Sweeper 和用户操作并发，也不会出现状态冲突
