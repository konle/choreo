# 容器子任务重试事件化方案（方案 C）

## 1. 问题描述

### 1.1 当前 Bug

Parallel 容器内多个子任务失败后，用户连续重试多个子任务时：
- 第一个重试成功：父工作流 `Failed → Pending`
- 第二个重试失败：`400 "workflow instance must be Failed or Await to retry container child, got Pending"`

### 1.2 根因

`retry_workflow_node` 在 API 层直接修改父工作流实例状态。第一个重试将父状态从 `Failed` 改为 `Pending`，后续重试因前置检查（`must be Failed or Await`）被拒绝。

### 1.3 设计矛盾

当前实现将"重试容器子任务"视为对**父工作流**的操作，但实际上它本质是对**子 TaskInstance** 的操作。操作对象错位导致了多个并发重试之间的状态冲突。

---

## 2. 方案设计

### 2.1 核心思想

**将"重试容器子任务"拆解为两个独立操作：**

1. **操作子 TaskInstance**：通过子任务自身的 CAS 重置状态（`Failed → Pending`），dispatch 子任务执行
2. **通知父工作流**：向 Workflow Worker 投递 `RetryContainerChild` 事件，Worker 在持有 lock 时安全地修改容器状态

两个子任务的重试**互不干扰**——因为操作的是各自独立的 TaskInstance（有独立的 CAS/epoch），而对父工作流的修改被序列化在 Worker 的事件处理队列中。

### 2.2 和现有架构的一致性

当前系统的事件模型：

```
API 层投递事件 → 队列 → Worker 消费 → acquire_lock → 修改实例 → release_lock
```

所有对工作流实例的写操作都通过 Worker 完成：
- `WorkflowEvent::Start` → Worker 启动执行
- `WorkflowEvent::NodeCallback` → Worker 处理子任务回调
- `WorkflowEvent::ChildRevived` → Worker 处理子工作流复活

新增 `RetryContainerChild` 遵循同一模型：API 不直接改父实例，而是通过事件让 Worker 安全处理。

---

## 3. 新增事件定义

### 3.1 WorkflowEvent 新增变体

```rust
// src/crates/domain/src/shared/job.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowEvent {
    Start,
    NodeCallback { ... },
    ChildRevived { node_id: String, child_id: String },
    
    /// 用户重试容器子任务的指令事件
    /// API 层投递，Worker 在持有 lock 后安全处理容器 state 回退
    RetryContainerChild {
        /// 容器节点 ID
        node_id: String,
        /// 被重试的子任务 ID
        child_task_id: String,
    },
}
```

---

## 4. API 层实现

### 4.1 retry_workflow_node 改造

```rust
// src/crates/domain/src/workflow/service.rs

pub async fn retry_workflow_node(
    &self,
    workflow_instance_id: &str,
    node_id: &str,
    child_task_id: Option<&str>,
) -> Result<(), String> {
    let inst = self.get_workflow_instance(workflow_instance_id.to_string()).await
        .map_err(|e| e.to_string())?;
    
    let node = inst.nodes.iter()
        .find(|n| n.node_id == node_id)
        .ok_or_else(|| format!("node {} not found", node_id))?;
    
    let is_container = matches!(
        node.node_type,
        TaskType::Parallel | TaskType::ForkJoin
    );
    
    if is_container {
        // ===== 容器子任务重试：事件化路径 =====
        self.retry_container_child_via_event(
            workflow_instance_id,
            node_id,
            child_task_id.ok_or("child_task_id required for container retry")?,
            &inst,
        ).await
    } else {
        // ===== 原子节点重试：沿用现有逻辑 =====
        // (Failed → Pending + dispatch Start)
        self.retry_atomic_node(workflow_instance_id, &inst).await
    }
}

/// 容器子任务重试的事件化实现
async fn retry_container_child_via_event(
    &self,
    workflow_instance_id: &str,
    node_id: &str,
    child_task_id: &str,
    inst: &WorkflowInstanceEntity,
) -> Result<(), String> {
    // 前置校验：父工作流必须不在终态 Completed/Canceled
    if matches!(inst.status, 
        WorkflowInstanceStatus::Completed | WorkflowInstanceStatus::Canceled
    ) {
        return Err(format!(
            "cannot retry container child: workflow is {:?}", 
            inst.status
        ));
    }
    
    // 1. 验证子任务确实是 Failed
    let child_task = self.task_instance_svc
        .get_task_instance_entity(child_task_id.to_string())
        .await
        .map_err(|e| format!("child task not found: {}", e))?;
    
    if child_task.task_status != TaskInstanceStatus::Failed {
        return Err(format!(
            "child task {} is not Failed, got {:?}", 
            child_task_id, child_task.task_status
        ));
    }
    
    // 2. CAS 重置子 TaskInstance: Failed → Pending
    //    这里使用子任务自己的 CAS，不涉及父工作流实例
    self.task_instance_svc
        .reset_to_pending(child_task_id)
        .await
        .map_err(|e| format!("failed to reset child task: {}", e))?;
    
    // 3. Dispatch ExecuteTaskJob（让子任务跑起来）
    self.dispatcher
        .dispatch_task(ExecuteTaskJob {
            task_instance_id: child_task_id.to_string(),
            tenant_id: inst.tenant_id.clone(),
            caller_context: Some(WorkflowCallerContext {
                workflow_instance_id: workflow_instance_id.to_string(),
                node_id: node_id.to_string(),
                parent_task_instance_id: None,
                item_index: None,
            }),
        })
        .await
        .map_err(|e| format!("failed to dispatch task: {}", e))?;
    
    // 4. Dispatch RetryContainerChild 事件给 Workflow Worker
    //    Worker 在持有 lock 后安全地 rollback 容器 state + 恢复父状态
    self.dispatcher
        .dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: workflow_instance_id.to_string(),
            tenant_id: inst.tenant_id.clone(),
            event: WorkflowEvent::RetryContainerChild {
                node_id: node_id.to_string(),
                child_task_id: child_task_id.to_string(),
            },
        })
        .await
        .map_err(|e| format!("failed to dispatch retry event: {}", e))?;
    
    Ok(())
}
```

### 4.2 API 响应

- 成功：`200 OK`（子任务已跑起来，父状态恢复在异步进行）
- 子任务不是 Failed：`400 Bad Request`
- 父工作流已完成：`400 Bad Request`
- 子任务 CAS 失败（被其他人重试了）：`409 Conflict`

---

## 5. Worker 层实现

### 5.1 process_workflow_job 新增分支

```rust
// src/crates/domain/src/plugin/manager/workflow.rs

let result = match job.event {
    WorkflowEvent::Start => { ... }
    WorkflowEvent::NodeCallback { ... } => { ... }
    WorkflowEvent::ChildRevived { ... } => { ... }
    WorkflowEvent::RetryContainerChild { node_id, child_task_id } => {
        self.on_retry_container_child(
            &job.workflow_instance_id, 
            &mut instance, 
            &node_id, 
            &child_task_id,
        ).await
    }
};
```

### 5.2 on_retry_container_child 实现

```rust
/// 处理容器子任务重试事件
/// 
/// Worker 持有 lock 时执行，安全修改容器 state 和父工作流状态
async fn on_retry_container_child(
    &self,
    workflow_instance_id: &str,
    instance: &mut WorkflowInstanceEntity,
    node_id: &str,
    child_task_id: &str,
) -> anyhow::Result<()> {
    let node = instance.nodes.iter_mut()
        .find(|n| n.node_id == *node_id)
        .ok_or_else(|| anyhow::anyhow!(
            "RetryContainerChild: node {} not found", node_id
        ))?;
    
    // Rollback 容器 state 中对应子任务的计数
    Self::rollback_child_in_container(node, child_task_id);
    
    match instance.status {
        WorkflowInstanceStatus::Failed => {
            // 父还在 Failed → 需要恢复
            // 清除节点错误状态
            let node = instance.nodes.iter_mut()
                .find(|n| n.node_id == *node_id)
                .unwrap();
            node.status = NodeExecutionStatus::Pending;
            node.error_message = None;
            
            // 状态转换：Failed → Pending
            let transition_result = instance
                .transition_status(WorkflowInstanceStatus::Pending)
                .map_err(|e| anyhow::anyhow!(
                    "RetryContainerChild transition error: {}", e
                ))?;
            
            self.save_instance_and_bump_epoch(instance).await?;
            
            // Dispatch outbound events (ChildRevived to grandparent if nested)
            for job in transition_result.into_dispatch_jobs() {
                if let Err(e) = self.dispatcher.dispatch_workflow(job).await {
                    warn!(
                        workflow_instance_id = %workflow_instance_id,
                        error = %e,
                        "failed to dispatch outbound ChildRevived to grandparent"
                    );
                }
            }
            
            // Dispatch Start to self（触发 re-evaluation）
            info!(
                workflow_instance_id = %workflow_instance_id,
                node_id = %node_id,
                child_task_id = %child_task_id,
                "RetryContainerChild: Failed→Pending, dispatching Start"
            );
            self.dispatcher
                .dispatch_workflow(ExecuteWorkflowJob {
                    workflow_instance_id: workflow_instance_id.to_string(),
                    tenant_id: instance.tenant_id.clone(),
                    event: WorkflowEvent::Start,
                })
                .await?;
        }
        WorkflowInstanceStatus::Await => {
            // 父在 Await → 只 rollback 计数，不改父状态
            self.save_instance_and_bump_epoch(instance).await?;
            debug!(
                workflow_instance_id = %workflow_instance_id,
                node_id = %node_id,
                child_task_id = %child_task_id,
                "RetryContainerChild: parent Await, rollback only"
            );
        }
        WorkflowInstanceStatus::Pending | WorkflowInstanceStatus::Running => {
            // 父已在恢复中（之前的 RetryContainerChild 触发了 Failed→Pending）
            // 只 rollback 计数
            self.save_instance_and_bump_epoch(instance).await?;
            debug!(
                workflow_instance_id = %workflow_instance_id,
                node_id = %node_id,
                child_task_id = %child_task_id,
                status = ?instance.status,
                "RetryContainerChild: parent recovering, rollback only"
            );
        }
        _ => {
            // Completed/Canceled → 忽略
            debug!(
                workflow_instance_id = %workflow_instance_id,
                status = ?instance.status,
                "RetryContainerChild ignored: terminal state"
            );
        }
    }
    
    Ok(())
}
```

---

## 6. 安全性分析

### 6.1 Worker 独占性

| 环节 | 安全性 | 原因 |
|------|--------|------|
| API 修改子 TaskInstance | ✅ | 子 TaskInstance 有独立 CAS，和父实例无关 |
| Worker 修改父实例 | ✅ | acquire_lock 保证独占 |
| 多个 RetryContainerChild 事件 | ✅ | Worker 串行消费，逐个 acquire_lock |

### 6.2 CAS 竞争

| 场景 | 是否冲突 | 原因 |
|------|---------|------|
| 两个子任务同时重试 | ❌ | 各自操作独立的 TaskInstance CAS |
| RetryContainerChild 和 NodeCallback 同时到 | ❌ | Worker 串行处理（acquire_lock 排队）|
| RetryContainerChild 和 Start 同时到 | ❌ | Worker 串行处理 |
| API 和 Worker 同时写父实例 | ❌ | API 不写父实例！ |

### 6.3 事件丢失

| 场景 | 是否丢失 | 原因 |
|------|---------|------|
| 30 个并发重试 | ❌ | Worker 串行消费所有事件 |
| Start 先于 RetryContainerChild 被消费 | ❌ | 后续 RetryContainerChild rollback 正常执行 |
| RetryContainerChild 先于 Start 被消费 | ❌ | 只有第一个触发 Failed→Pending + dispatch Start |
| 队列消息丢失 | ⚠️ | Sweeper 兜底（扫描 Await/Pending 状态实例）|

### 6.4 幂等性

| 场景 | 行为 |
|------|------|
| 重复 dispatch 同一个 RetryContainerChild | rollback_child_in_container 幂等（已 rollback 的不会重复减计数）|
| 父已完成后收到 RetryContainerChild | 被忽略 |
| 子任务已经不是 Failed（被重试过了）| API 前置检查拒绝，或 CAS 失败 |

---

## 7. 时序演示

### 7.1 正常场景：两个子任务连续重试

```
t0: 父 Failed, 子-0 Failed, 子-1 Failed
    容器 state: {failed_count: 2, processed_callbacks: [子-0, 子-1]}

t1: 用户重试子-0
    API: 子-0 CAS(Failed→Pending) ✓
    API: dispatch task(子-0) ✓
    API: dispatch RetryContainerChild(子-0) ✓
    → 200 OK

t2: 用户重试子-1（几毫秒后）
    API: 子-1 CAS(Failed→Pending) ✓
    API: dispatch task(子-1) ✓
    API: dispatch RetryContainerChild(子-1) ✓
    → 200 OK

t3: Worker 消费 RetryContainerChild(子-0)
    acquire_lock → 读取父(Failed, epoch=N)
    rollback 子-0: failed_count: 2→1, processed_callbacks remove 子-0, results[子-0]=null
    Failed→Pending
    save(epoch=N+1)
    dispatch Start
    release_lock

t4: Worker 消费 RetryContainerChild(子-1)
    acquire_lock → 读取父(Pending, epoch=N+1)
    rollback 子-1: failed_count: 1→0, processed_callbacks remove 子-1, results[子-1]=null
    父已 Pending, 只 save(epoch=N+2)
    release_lock

t5: Worker 消费 Start
    acquire_lock → 读取父(Pending, epoch=N+2)
    Pending→Running
    re-evaluation: dispatched_count=2, 子任务已 dispatch → Await
    Running→Await
    save(epoch=N+3)
    release_lock

t6: 子-0 完成 → NodeCallback(Success)
    acquire_lock → Await→Running
    handle_callback: success_count: 0→1
    all_done? 1+0≠2 → Await
    release_lock

t7: 子-1 完成 → NodeCallback(Success)
    acquire_lock → Await→Running
    handle_callback: success_count: 1→2
    all_done? 2+0==2 → Success!
    容器完成 → 工作流继续下一个节点
```

### 7.2 极端场景：子任务在 Worker 处理 RetryContainerChild 之前就完成了

```
t1: 用户重试子-0
    API: 子-0 → Pending, dispatch task, dispatch RetryContainerChild

t2: 子-0 执行非常快，立即完成 → dispatch NodeCallback(Success)

t3: Worker 先消费 NodeCallback(子-0 Success)
    acquire_lock → 父 Failed
    prepare_instance: Failed → Ignored!（callback 被忽略）
    release_lock

t4: Worker 消费 RetryContainerChild(子-0)
    acquire_lock → 父 Failed
    rollback 子-0
    Failed→Pending + dispatch Start
    release_lock

t5: Worker 消费 Start → re-evaluation → Await

    → 此时子-0 已完成但 callback 被丢弃了!
    → 需要 Sweeper 或 Stale Check 兜底
```

**这个极端时序需要关注。** 解决方案见第 8 节。

---

## 8. 极端时序的兜底

### 8.1 问题

如果子任务执行极快（< 100ms），可能在 Worker 处理 `RetryContainerChild` 之前就完成了。此时 `NodeCallback` 可能在父还是 `Failed` 状态时到达 → 被 `prepare_instance_for_node_callback` 忽略（Failed 状态不接受 callback）。

### 8.2 兜底机制

**方案 1：修改 `prepare_instance_for_node_callback` 的 Failed 状态处理**

对于 Failed 状态收到的 callback，不直接忽略，而是**暂存**或**重新投递到队列尾部**（延迟处理）：

```rust
WorkflowInstanceStatus::Failed => {
    // 可能有 RetryContainerChild 事件在队列中等待处理
    // 重新 dispatch 这个 callback，延迟处理
    warn!("callback received while Failed, re-dispatching for later processing");
    self.dispatcher.dispatch_workflow(ExecuteWorkflowJob {
        workflow_instance_id: instance.workflow_instance_id.clone(),
        tenant_id: instance.tenant_id.clone(),
        event: WorkflowEvent::NodeCallback { ... }, // 原样重发
    }).await?;
    Ok(CallbackReadiness::Ignored)
}
```

**问题**：如果不存在 RetryContainerChild 事件（子任务自己完成了而非重试），会无限重发。需要限制重试次数。

**方案 2：依赖 Sweeper 兜底（推荐）**

不修改 `prepare_instance_for_node_callback`。当 Worker 处理 `RetryContainerChild` 后父变为 Await，Sweeper 在扫描时发现：
- 父是 Await
- 子-0 在 DB 中已经是 Completed
- 但 processed_callbacks 中没有子-0（被 rollback 了）

Sweeper 补发 callback → 正常处理。

**等待时间：最多 60s（Sweeper 周期）。**

**方案 3：RetryContainerChild 处理时主动检查（推荐）**

Worker 处理 `RetryContainerChild` 时，在 rollback 之后**检查子任务的 DB 状态**：

```rust
// 在 on_retry_container_child 中，rollback 完成后:
let child_status = self.task_instance_svc
    .get_task_instance_entity(child_task_id.to_string())
    .await?;

if child_status.task_status.is_terminal() {
    // 子任务已经完成了！主动补发 callback
    self.dispatcher.dispatch_workflow(ExecuteWorkflowJob {
        workflow_instance_id: workflow_instance_id.to_string(),
        tenant_id: instance.tenant_id.clone(),
        event: WorkflowEvent::NodeCallback {
            node_id: node_id.to_string(),
            child_task_id: child_task_id.to_string(),
            status: if child_status.task_status == TaskInstanceStatus::Completed {
                NodeExecutionStatus::Success
            } else {
                NodeExecutionStatus::Failed
            },
            output: child_status.output,
            error_message: child_status.error_message,
            input: child_status.input,
        },
    }).await?;
}
```

**这是最健壮的方案：Worker 在处理重试事件时主动补偿可能丢失的 callback，无需等待 Sweeper。**

### 8.3 推荐方案

**方案 3（主动检查 + 补发 callback）** 作为首选，Sweeper 作为兜底。

---

## 9. 与 ChildRevived 事件的关系

| 事件 | 来源 | 目的 | 场景 |
|------|------|------|------|
| `ChildRevived` | 子 WorkflowInstance 的 `transition_status` | 通知父"子工作流活了" | SubWorkflow 节点的子工作流被重试 |
| `RetryContainerChild` | API 层 `retry_workflow_node` | 指令 Worker "用户要重试这个容器子任务" | Parallel/ForkJoin 容器内的子任务重试 |

两者的区别：
- `ChildRevived` 是**子→父的通知**（状态的固有属性，自动产生）
- `RetryContainerChild` 是**API→Worker 的指令**（用户操作触发，需要显式投递）

---

## 10. TaskInstanceService 新增方法

```rust
/// 重置任务实例状态为 Pending（用于重试）
/// 使用子任务自身的 CAS 保证安全
pub async fn reset_to_pending(&self, task_instance_id: &str) -> Result<(), String> {
    let task = self.get_task_instance_entity(task_instance_id.to_string())
        .await
        .map_err(|e| e.to_string())?;
    
    if task.task_status != TaskInstanceStatus::Failed {
        return Err(format!(
            "task {} is not Failed, got {:?}", 
            task_instance_id, task.task_status
        ));
    }
    
    // CAS: 只有当前 status 仍为 Failed 时才更新
    self.repository
        .reset_status_to_pending(task_instance_id, &TaskInstanceStatus::Failed)
        .await
        .map_err(|e| format!("CAS failed: {}", e))
}
```

Repository 层：
```rust
/// CAS 重置 task_status: 只有当前 status == expected_status 时才更新为 Pending
async fn reset_status_to_pending(
    &self,
    task_instance_id: &str,
    expected_status: &TaskInstanceStatus,
) -> Result<(), RepositoryError>;

// MongoDB 实现:
async fn reset_status_to_pending(
    &self,
    task_instance_id: &str,
    expected_status: &TaskInstanceStatus,
) -> Result<(), RepositoryError> {
    let filter = doc! {
        "task_instance_id": task_instance_id,
        "task_status": bson::to_bson(expected_status)?,
    };
    let update = doc! {
        "$set": {
            "task_status": bson::to_bson(&TaskInstanceStatus::Pending)?,
            "updated_at": bson::DateTime::now(),
            "error_message": null,
        }
    };
    let result = self.collection.update_one(filter, update).await?;
    if result.matched_count == 0 {
        return Err(RepositoryError::CasFailed(
            "task instance status changed concurrently".into()
        ));
    }
    Ok(())
}
```

---

## 11. rollback_child_in_container 幂等性

```rust
fn rollback_child_in_container(
    node: &mut WorkflowNodeInstanceEntity,
    child_task_id: &str,
) {
    let state = match &mut node.task_instance.output {
        Some(s) => s,
        None => return,
    };
    
    // 检查是否在 processed_callbacks 中（幂等：不在就不操作）
    let in_processed = state
        .get("processed_callbacks")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().any(|v| v.as_str() == Some(child_task_id)))
        .unwrap_or(false);
    
    if !in_processed {
        // 已经 rollback 过（或从未记录过）→ 幂等，不重复操作
        return;
    }
    
    // 从 processed_callbacks 移除
    if let Some(arr) = state.get_mut("processed_callbacks").and_then(|v| v.as_array_mut()) {
        arr.retain(|v| v.as_str() != Some(child_task_id));
    }
    
    // 检查 results 中的记录
    let prev_status = state
        .get("results")
        .and_then(|r| r.get(child_task_id))
        .and_then(|e| e.get("status"))
        .and_then(|s| s.as_str())
        .unwrap_or("");
    
    // 回退对应计数器
    match prev_status {
        "Failed" => {
            let fc = state["failed_count"].as_u64().unwrap_or(0);
            state["failed_count"] = serde_json::json!(fc.saturating_sub(1));
        }
        "Success" => {
            let sc = state["success_count"].as_u64().unwrap_or(0);
            state["success_count"] = serde_json::json!(sc.saturating_sub(1));
        }
        _ => {}
    }
    
    // 清除 results 中的记录
    if let Some(results) = state.get_mut("results").and_then(|r| r.as_object_mut()) {
        results.insert(child_task_id.to_string(), serde_json::Value::Null);
    }
}
```

**幂等保证**：如果 child_task_id 不在 processed_callbacks 中（已被 rollback 过），函数直接返回，不会重复减计数。

---

## 12. 与现有 Stale Failure Check 的交互

Stale Failure Check 在 `handle_callback` 中检查 results 中标记为 "Failed" 的子任务。

在 `RetryContainerChild` 处理后：
- results[child_task_id] 已被设为 null（rollback 清除了）
- processed_callbacks 已移除 child_task_id

所以当后续 callback 到来时，Stale Check 不会再检查已被 rollback 的子任务。两者互不干扰。

---

## 13. 文件变更清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `domain/shared/job.rs` | 修改 | 新增 `WorkflowEvent::RetryContainerChild` 变体 |
| `domain/workflow/service.rs` | 修改 | `retry_workflow_node` 拆分为原子节点路径和容器事件化路径 |
| `domain/plugin/manager/workflow.rs` | 修改 | `process_workflow_job` 新增 match 分支 + `on_retry_container_child` |
| `domain/task/service.rs` | 修改 | 新增 `reset_to_pending` 方法 |
| `domain/task/repository.rs` (trait) | 修改 | 新增 `reset_status_to_pending` |
| `infrastructure/.../task_repo.rs` | 修改 | MongoDB CAS 实现 |

---

## 14. 测试计划

### 14.1 单元测试

| 测试 | 说明 |
|------|------|
| `test_retry_container_child_api_resets_task` | API 层正确重置子 TaskInstance 状态 |
| `test_retry_container_child_api_dispatches_event` | API 层正确投递 RetryContainerChild 事件 |
| `test_retry_container_child_api_rejects_non_failed` | 子任务不是 Failed 时返回错误 |
| `test_retry_container_child_api_rejects_completed_workflow` | 父工作流已完成时返回错误 |
| `test_on_retry_container_child_from_failed` | Worker: Failed → rollback + Pending + Start |
| `test_on_retry_container_child_from_await` | Worker: Await → rollback only |
| `test_on_retry_container_child_from_pending` | Worker: Pending → rollback only |
| `test_on_retry_container_child_idempotent` | Worker: 重复事件不会重复 rollback |
| `test_on_retry_container_child_checks_finished_task` | Worker: 子任务已完成时补发 callback |
| `test_rollback_child_in_container_idempotent` | rollback 函数幂等性 |

### 14.2 集成测试

| 测试 | 说明 |
|------|------|
| `test_parallel_retry_two_failed_children` | 两个子任务都失败，连续重试两个，最终完成 |
| `test_parallel_retry_during_recovery` | 重试时父已在 Pending，第二个重试仍成功 |
| `test_forkjoin_retry_container_child` | ForkJoin 场景下的容器子任务重试 |

---

## 15. 对比总结

| 维度 | 旧方案（直接改父） | 新方案（事件化） |
|------|-----------------|----------------|
| 并发重试 | ❌ 409 | ✅ 全部成功 |
| Worker 独占性 | ⚠️ API 和 Worker 可能冲突 | ✅ 所有写操作都在 Worker |
| CAS 竞争 | ⚠️ 可能 | ✅ 无（子任务独立 CAS + Worker 串行）|
| 架构一致性 | ⚠️ API 直接改实例 | ✅ 和 Start/Callback 模式一致 |
| 前端适配 | 需要处理 409 | 无需改动（200 OK）|
| 极端时序 | 不适用 | Sweeper 兜底 + Worker 主动检查 |
