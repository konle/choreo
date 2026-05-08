# 统一事件协议设计文档：状态转换驱动的出站事件

## 1. 问题陈述

### 1.1 现状

当前工作流引擎的父子通信机制是**点对点、显式调用、场景驱动**的：

- Task Worker 完成后 → 代码里写死发 `NodeCallback`
- SubWorkflow 完成后 → `notify_parent_if_needed` 只在终态通知
- Parallel 子任务重试 → `retry_workflow_node` 手动写 `Failed → Await` 级联
- SubWorkflow 重试 → 无人负责通知父 ❌

这种"各调用者自行负责通知"的模式存在系统性缺陷：

1. **遗漏风险**：每新增一个操作（重试、取消、跳过、超时），开发者必须记得在对应代码路径手动加通知，遗漏 = 死锁
2. **不一致**：同是重试，Parallel 子任务有级联，SubWorkflow 没有
3. **不可测试**：通知逻辑散落在十几个函数中，无法单元测试覆盖完整性

### 1.2 典型故障场景

```
父工作流 (Parallel容器，含混合子任务：HTTP + SubWorkflow)
  ├── HTTP-Task-0 ~ HTTP-Task-97 (全部成功)
  ├── SubWorkflow-98 (成功)
  └── SubWorkflow-99 (失败) ← 触发 max_failures → Parallel Failed → 父 Failed

用户重试 SubWorkflow-99 内部的失败节点：
  SubWorkflow-99: Failed → Pending → Running → ... → Completed
  SubWorkflow-99 完成后 → notify_parent_if_needed → 向父发 NodeCallback
  父收到 NodeCallback → 父状态是 Failed → prepare_instance_for_node_callback 拒绝 → 回调被丢弃
  
结果：父永远停在 Failed，即使所有子任务实际都已成功
```

### 1.3 根因

**通知是调用者的责任**（谁触发状态变更，谁负责通知），而非**状态转换的固有属性**。

---

## 2. 设计目标

| 目标 | 说明 |
|------|------|
| **不可遗漏** | 出站事件由状态转换自动产生，不依赖调用者记得写通知代码 |
| **一致性** | 无论是 retry/skip/cancel/timeout 触发的同一状态转换，产生的出站事件完全相同 |
| **可测试** | 出站规则是纯函数，可 100% 单元测试覆盖 |
| **零架构破坏** | 保留现有 Apalis 队列、epoch/CAS、Worker 模型，不引入事件溯源 |
| **全类型覆盖** | 容器节点（Parallel/ForkJoin）内的所有子任务类型（HTTP、gRPC、SubWorkflow）统一支持；独立节点（非容器）的所有类型同样支持 |
| **渐进式** | 可分阶段落地，每个阶段独立可验证 |

---

## 3. 核心概念

### 3.1 设计哲学

> **"通知不是调用者的责任，而是状态转换的固有属性。"**

无论是谁、因为什么原因触发了 `Failed → Pending` 转换（retry API、sweeper、未来的任何新功能），出站事件**只取决于 (old_state, new_state) 这个二元组**，不取决于触发者。

### 3.2 两个互补机制

本方案由两个互补机制共同保证正确性：

| 机制 | 职责 | 触发时机 |
|------|------|---------|
| **出站事件（Revived/Terminated）** | 主动通知父，驱动父状态恢复 | 子实体状态转换时 |
| **Stale Failure Check（过期失败检查）** | 自修正，消除竞争窗口 | 容器 handle_callback 每次执行时 |

**为什么需要两个机制？**

单靠 Revived 事件存在竞争窗口：子被重试后、Revived 到达父之前，其他子任务的 callback 可能导致容器误判 all_done。Stale Failure Check 作为防御性校验，确保即使事件时序不理想，容器状态机仍然正确。

### 3.3 适用范围

| 节点类型 | 子任务类型 | 出站事件来源 | Stale Check 对象 |
|---------|-----------|-------------|-----------------|
| **独立 SubWorkflow** | 子工作流实例 | 子 WorkflowInstance 状态转换 | 子 WorkflowInstance |
| **独立 HTTP/gRPC** | TaskInstance | Task Worker 投递 NodeCallback（不变） | 不适用（单子任务） |
| **Parallel/ForkJoin 内 HTTP/gRPC** | TaskInstance | Task Worker 投递 NodeCallback（不变） | task_instances 集合 |
| **Parallel/ForkJoin 内 SubWorkflow** | 子工作流实例 | 子 WorkflowInstance 状态转换 | workflow_instances 集合 |

### 3.4 术语

| 术语 | 定义 |
|------|------|
| **状态转换** | 实体（WorkflowInstance / TaskInstance）的 status 字段从一个值变为另一个值 |
| **出站事件 (Outbound Event)** | 状态转换后自动产生的、需要通知给关联方的事件 |
| **关联方** | 通过 `parent_context` / `caller_context` 建立的父子关系中的父方 |
| **转换入口** | 所有状态变更必须经过的唯一函数 |
| **Stale Failure** | 容器状态机中标记为 Failed 但实际已被重试（不再是 Failed）的子任务 |

### 3.5 与现有概念的映射

| 现有概念 | 新模型中的等价物 |
|---------|-----------------|
| `NodeCallback(Success/Failed)` | `ChildEvent::Terminated`（对 Task 不变；对 SubWorkflow 由 transition_status 自动产生） |
| `retry_workflow_node` 中手动的 `Failed→Await` 级联 | 被 Revived 事件 + 父侧 `Failed→Pending→Running→gather→Await` 取代 |
| `notify_parent_if_needed` | 被 `transition_status()` 的自动出站逻辑取代 |

---

## 4. 出站事件协议

### 4.1 子事件类型 (ChildEvent)

```rust
/// 子实体向父实体发送的事件
pub enum ChildEvent {
    /// 子实体到达终态（等价于当前 NodeCallback）
    Terminated {
        status: TerminalStatus,
        output: Option<serde_json::Value>,
        error_message: Option<String>,
        input: Option<serde_json::Value>,
    },
    
    /// 子实体离开终态（重试/恢复）— 当前缺失的关键事件
    Revived,
}

pub enum TerminalStatus {
    Completed,
    Failed,
}
```

### 4.2 出站规则表 (should_notify_parent)

**WorkflowInstance 状态转换的出站规则：**

| from | to | 产生事件 | 说明 |
|------|----|---------|------|
| `Running` | `Completed` | `Terminated { Completed, output, ... }` | 子工作流成功 |
| `Running` | `Failed` | `Terminated { Failed, ..., error }` | 子工作流失败 |
| `Failed` | `Pending` | `Revived` | 子工作流被重试（离开终态） |
| `Pending` | `Running` | — | 无需通知（执行开始不影响父状态） |
| `Running` | `Await` | — | 无需通知（内部等待不影响父） |
| `Running` | `Suspended` | — | 无需通知（内部挂起） |
| `Await` | `Pending` | — | 无需通知（唤醒是内部行为） |
| `Suspended` | `Pending` | — | 无需通知（恢复是内部行为） |
| `*` | `Canceled` | `Terminated { Failed, error: "canceled" }` | 取消等价于失败终态 |

**TaskInstance 状态转换的出站规则：**

| from | to | 产生事件 | 说明 |
|------|----|---------|------|
| `Running` | `Completed` | `Terminated { Completed, output }` | 任务成功（当前由 Task Worker 手动投递，Phase 3 统一） |
| `Running` | `Failed` | `Terminated { Failed, error }` | 任务失败 |
| `Failed` | `Pending` | `Revived` | 任务被重试 |
| `*` | `Canceled` | `Terminated { Failed, error: "canceled" }` | 取消 |

**出站前提条件：** 实体必须具有 `parent_context`（WorkflowInstance）或 `caller_context`（TaskInstance）。无关联方的实体（根工作流、独立任务）不产生出站事件。

### 4.3 纯函数规范

```rust
/// 判定状态转换是否需要通知父实体
/// 纯函数，零 IO，完全可测试
pub fn should_notify_parent(
    old_status: &WorkflowInstanceStatus,
    new_status: &WorkflowInstanceStatus,
) -> Option<ChildEventKind> {
    use WorkflowInstanceStatus::*;
    match (old_status, new_status) {
        // 进入终态 → 通知
        (_, Completed) => Some(ChildEventKind::Terminated(TerminalStatus::Completed)),
        (_, Failed) => Some(ChildEventKind::Terminated(TerminalStatus::Failed)),
        (_, Canceled) => Some(ChildEventKind::Terminated(TerminalStatus::Failed)),
        
        // 离开终态 → 通知
        (Failed, Pending) => Some(ChildEventKind::Revived),
        
        // 其他转换 → 不通知
        _ => None,
    }
}
```

---

## 5. 统一状态转换层

### 5.1 接口设计

```rust
pub struct StateTransitionResult {
    /// 需要投递给父工作流的出站事件
    pub outbound_events: Vec<OutboundEvent>,
}

pub struct OutboundEvent {
    pub target_workflow_id: String,
    pub target_tenant_id: String,
    pub event: WorkflowEvent,
}

impl WorkflowInstanceEntity {
    /// 执行状态转换并自动计算出站事件
    /// 所有状态变更必须经过此函数
    pub fn transition_status(
        &mut self,
        new_status: WorkflowInstanceStatus,
    ) -> Result<StateTransitionResult, TransitionError> {
        let old_status = self.status.clone();
        
        // 1. 合法性校验
        validate_workflow_transition(&old_status, &new_status)?;
        
        // 2. 执行转换
        self.status = new_status.clone();
        self.updated_at = Utc::now();
        
        // 3. 计算出站事件
        let mut outbound_events = vec![];
        if let Some(parent_ctx) = &self.parent_context {
            if let Some(event_kind) = should_notify_parent(&old_status, &new_status) {
                let workflow_event = self.build_parent_event(parent_ctx, event_kind);
                outbound_events.push(OutboundEvent {
                    target_workflow_id: parent_ctx.workflow_instance_id.clone(),
                    target_tenant_id: self.tenant_id.clone(),
                    event: workflow_event,
                });
            }
        }
        
        Ok(StateTransitionResult { outbound_events })
    }
}
```

### 5.2 事件构造

```rust
impl WorkflowInstanceEntity {
    fn build_parent_event(
        &self,
        parent_ctx: &WorkflowCallerContext,
        event_kind: ChildEventKind,
    ) -> WorkflowEvent {
        match event_kind {
            ChildEventKind::Terminated(terminal_status) => {
                let status = match terminal_status {
                    TerminalStatus::Completed => NodeExecutionStatus::Success,
                    TerminalStatus::Failed => NodeExecutionStatus::Failed,
                };
                WorkflowEvent::NodeCallback {
                    node_id: parent_ctx.node_id.clone(),
                    child_task_id: self.workflow_instance_id.clone(),
                    status,
                    output: Some(self.context.clone()),
                    error_message: None,
                    input: None,
                }
            }
            ChildEventKind::Revived => {
                WorkflowEvent::ChildRevived {
                    node_id: parent_ctx.node_id.clone(),
                    child_id: self.workflow_instance_id.clone(),
                }
            }
        }
    }
}
```

### 5.3 WorkflowEvent 扩展

```rust
pub enum WorkflowEvent {
    Start,
    NodeCallback {
        node_id: String,
        child_task_id: String,
        status: NodeExecutionStatus,
        output: Option<serde_json::Value>,
        error_message: Option<String>,
        input: Option<serde_json::Value>,
    },
    /// 新增：子实体离开终态（被重试/恢复）
    ChildRevived {
        node_id: String,
        child_id: String,
    },
}
```

### 5.4 调用模式

```rust
// 所有状态变更统一经过 transition_status
async fn some_operation(&self, instance: &mut WorkflowInstanceEntity) -> Result<...> {
    // 状态转换通过统一入口
    let transition_result = instance.transition_status(WorkflowInstanceStatus::Pending)?;
    
    // 持久化（先落库）
    self.save_instance(instance).await?;
    
    // 投递出站事件（后投递）
    for event in transition_result.outbound_events {
        self.dispatcher.dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: event.target_workflow_id,
            tenant_id: event.target_tenant_id,
            event: event.event,
        }).await?;
    }
    
    Ok(...)
}
```

---

## 6. 父工作流侧处理：ChildRevived

### 6.1 process_workflow_job 扩展

```rust
pub async fn process_workflow_job(&self, job: ExecuteWorkflowJob) -> anyhow::Result<()> {
    match job.event {
        WorkflowEvent::Start => self.on_start(job).await,
        WorkflowEvent::NodeCallback { .. } => self.on_node_callback(job).await,
        WorkflowEvent::ChildRevived { .. } => self.on_child_revived(job).await,
    }
}
```

### 6.2 on_child_revived 处理逻辑

```rust
async fn on_child_revived(
    &self,
    workflow_instance_id: &str,
    node_id: &str,
    child_id: &str,
) -> anyhow::Result<()> {
    let mut instance = self.load_and_lock(workflow_instance_id).await?;
    
    match instance.status {
        WorkflowInstanceStatus::Failed => {
            // 父因子失败而 Failed → 恢复为 Pending，让 worker 重入做 gather
            self.revive_from_failed(&mut instance, node_id, child_id).await?;
        }
        WorkflowInstanceStatus::Await => {
            // 父本来就在等（子失败但未触发 max_failures）
            // 此时容器状态机需要回退该子的失败计数
            self.revive_from_await(&mut instance, node_id, child_id).await?;
        }
        _ => {
            warn!(
                workflow_instance_id = %workflow_instance_id,
                parent_status = ?instance.status,
                child_id = %child_id,
                "ChildRevived received but parent not in Failed/Await, ignoring"
            );
        }
    }
    
    Ok(())
}
```

### 6.3 revive_from_failed（核心恢复逻辑）

```rust
async fn revive_from_failed(
    &self,
    instance: &mut WorkflowInstanceEntity,
    node_id: &str,
    child_id: &str,
) -> anyhow::Result<()> {
    let node = instance.find_node_mut(node_id)?;
    
    match node.node_type {
        // 容器节点：回退计数器 + 恢复节点和工作流到 Pending
        TaskType::Parallel | TaskType::ForkJoin => {
            self.rollback_child_in_container(node, child_id)?;
            node.status = NodeExecutionStatus::Pending;
            instance.status = WorkflowInstanceStatus::Pending;
        }
        
        // 独立 SubWorkflow 节点：恢复节点和工作流到 Pending
        TaskType::SubWorkflow => {
            node.status = NodeExecutionStatus::Pending;
            instance.status = WorkflowInstanceStatus::Pending;
        }
        
        // 独立 HTTP/gRPC 节点：恢复到 Pending
        TaskType::Http | TaskType::Grpc | TaskType::Llm => {
            node.status = NodeExecutionStatus::Pending;
            instance.status = WorkflowInstanceStatus::Pending;
        }
        
        _ => {
            warn!("unexpected ChildRevived for node type {:?}", node.node_type);
            return Ok(());
        }
    }
    
    // 持久化
    self.save_workflow_instance(instance).await?;
    
    // 投递 Start 让 Worker 重入
    self.dispatcher.dispatch_workflow(ExecuteWorkflowJob {
        workflow_instance_id: instance.workflow_instance_id.clone(),
        tenant_id: instance.tenant_id.clone(),
        event: WorkflowEvent::Start,
    }).await?;
    
    Ok(())
}
```

### 6.4 revive_from_await

```rust
async fn revive_from_await(
    &self,
    instance: &mut WorkflowInstanceEntity,
    node_id: &str,
    child_id: &str,
) -> anyhow::Result<()> {
    let node = instance.find_node_mut(node_id)?;
    
    // 容器场景：子失败但未触发 max_failures，父仍在 Await
    // 只需回退计数器，不需要改变工作流状态
    if matches!(node.node_type, TaskType::Parallel | TaskType::ForkJoin) {
        self.rollback_child_in_container(node, child_id)?;
        self.save_workflow_instance(instance).await?;
    }
    // 非容器场景（独立 SubWorkflow 在 Await 时收到 Revived）：
    // 不应该发生（SubWorkflow 失败时父节点直接 Failed，不会留在 Await）
    // 如果发生则 warn 并忽略
    
    Ok(())
}
```

### 6.5 容器状态机回退

```rust
fn rollback_child_in_container(
    &self,
    node: &mut WorkflowNodeInstanceEntity,
    child_id: &str,
) -> anyhow::Result<()> {
    let state = self.load_container_state(node)?;
    
    // 幂等保护：如果 child_id 不在 processed_callbacks 中，说明已回退过
    if !state.processed_callbacks.contains(&child_id.to_string()) {
        return Ok(());
    }
    
    // 回退计数
    if let Some(result) = state.results.get(child_id) {
        if let Some(status) = result.get("status").and_then(|v| v.as_str()) {
            match status {
                "Failed" => state.failed_count -= 1,
                "Success" => state.success_count -= 1,
                "Skipped" => {
                    state.success_count -= 1;
                    state.skipped_count -= 1;
                }
                _ => {}
            }
        }
    }
    
    // 从 processed_callbacks 移除
    state.processed_callbacks.retain(|id| id != child_id);
    
    // 重置该 child 的结果
    state.results.insert(child_id.to_string(), serde_json::Value::Null);
    
    self.save_container_state(node, &state)?;
    Ok(())
}
```

---

## 7. Plugin Execute Re-evaluation（gather 机制）

### 7.1 设计原则

当父工作流因 ChildRevived 从 `Failed → Pending` 恢复并被 Worker 拉起（`Pending → Running`）后，execute_workflow_loop 遇到当前节点状态为 `Pending` 时，调用 `plugin.execute()`。

Plugin 需要区分**首次执行**和**re-evaluation（重入 gather）**：
- 首次执行：初始化状态机，scatter 派发子任务
- Re-evaluation：检查已有子任务的实际状态，决定 await/成功/失败

### 7.2 SubWorkflow Plugin Re-evaluation

```rust
impl SubWorkflowPlugin {
    async fn execute(&self, ...) -> ExecutionResult {
        // 检查是否已有子工作流实例
        let existing_child = self.find_existing_child_instance(
            workflow_instance, node_instance
        ).await?;
        
        match existing_child {
            Some(child) => {
                // Re-evaluation: 子实例已存在，查当前状态
                match child.status {
                    WorkflowInstanceStatus::Completed => {
                        // 子已完成 → 直接返回成功
                        ExecutionResult::success(Some(child.context))
                    }
                    WorkflowInstanceStatus::Failed => {
                        // 子仍失败（可能重试后又失败了）→ 报告失败
                        ExecutionResult::failed()
                    }
                    _ => {
                        // 子正在执行中（Pending/Running/Await/Suspended）→ 等回调
                        ExecutionResult::await_callback()
                    }
                }
            }
            None => {
                // 首次执行：创建子实例并 dispatch
                self.create_and_dispatch_child(...).await
            }
        }
    }
}
```

### 7.3 Parallel Plugin Re-evaluation

```rust
impl ParallelPlugin {
    async fn execute(&self, ...) -> ExecutionResult {
        let existing_state = self.load_existing_state(node_instance);
        
        match existing_state {
            Some(state) if state.dispatched_count > 0 => {
                // Re-evaluation: 状态机已初始化过
                self.reevaluate_parallel(state, node_instance).await
            }
            _ => {
                // 首次执行：正常初始化 + scatter
                self.initialize_and_dispatch(...).await
            }
        }
    }
    
    async fn reevaluate_parallel(
        &self,
        state: &ContainerState,
        node_instance: &WorkflowNodeInstanceEntity,
    ) -> ExecutionResult {
        // 查询所有子任务/子工作流的实际状态
        let actual_states = self.query_actual_child_states(node_instance).await?;
        
        let actual_completed = actual_states.iter()
            .filter(|s| s.is_terminal_success())
            .count();
        let actual_failed = actual_states.iter()
            .filter(|s| s.is_terminal_failure())
            .count();
        let actual_running = actual_states.iter()
            .filter(|s| !s.is_terminal())
            .count();
        
        let total = state.total_items as usize;
        
        if actual_completed + actual_failed == total {
            // 全部到达终态
            if actual_failed > 0 {
                ExecutionResult::failed()
            } else {
                ExecutionResult::success(...)
            }
        } else {
            // 还有子任务在跑 → 等回调
            ExecutionResult::await_callback()
        }
    }
}
```

### 7.4 ForkJoin Plugin Re-evaluation

与 Parallel 逻辑一致（共享 Scatter-Gather 状态机），区别仅在于子任务来源（静态列表 vs 动态数组）。

### 7.5 HTTP/gRPC Plugin Re-evaluation

```rust
impl HttpPlugin {
    async fn execute(&self, ...) -> ExecutionResult {
        // 检查是否已有 TaskInstance
        let existing_task = self.find_existing_task_instance(
            workflow_instance, node_instance
        ).await?;
        
        match existing_task {
            Some(task) => {
                // Re-evaluation
                match task.status {
                    TaskInstanceStatus::Completed => {
                        ExecutionResult::success(task.output)
                    }
                    TaskInstanceStatus::Failed => {
                        ExecutionResult::failed()
                    }
                    _ => {
                        // 任务正在执行中
                        ExecutionResult::await_callback()
                    }
                }
            }
            None => {
                // 首次执行：创建任务实例并 dispatch
                self.create_and_dispatch_task(...).await
            }
        }
    }
}
```

---

## 8. Stale Failure Check（过期失败检查）

### 8.1 设计动机

**竞争窗口**：子被重试（Revived 入队）后、Revived 到达父之前，其他子任务的 callback 可能触发容器的 "all_done" 判定，此时 failed_count 仍包含已被重试的子 → 误判为 Failed。

**解法**：容器的 `handle_callback` 每次执行时，对已标记为 Failed 的子做一次轻量级实际状态校验。如果发现某个子实际已不是 Failed（被重试了），立即修正计数。

### 8.2 适用场景

| 容器类型 | 子任务类型 | 检查对象 |
|---------|-----------|---------|
| Parallel | HTTP/gRPC | task_instances 集合中的 TaskInstance |
| Parallel | SubWorkflow | workflow_instances 集合中的 WorkflowInstance |
| ForkJoin | HTTP/gRPC | task_instances 集合中的 TaskInstance |
| ForkJoin | SubWorkflow | workflow_instances 集合中的 WorkflowInstance |

### 8.3 实现

```rust
impl ParallelPlugin {
    async fn handle_callback(&self, ...) -> ExecutionResult {
        // 1. 正常处理当前回调（累加计数器、幂等检查等）
        self.process_current_callback(&mut state, child_task_id, status, output)?;
        
        // 2. Stale Failure Check：之前标记为 Failed 的子任务是否有被重试的？
        let stale_failures = self.check_stale_failures(&state, node_instance).await?;
        if !stale_failures.is_empty() {
            self.reconcile_state(&mut state, &stale_failures);
        }
        
        // 3. 基于修正后的状态做完成判定
        self.evaluate_completion(&state)
    }
}
```

### 8.4 check_stale_failures

```rust
async fn check_stale_failures(
    &self,
    state: &ContainerState,
    node_instance: &WorkflowNodeInstanceEntity,
) -> anyhow::Result<Vec<String>> {
    // 找出 results 中标记为 Failed 的 child_id 列表
    let failed_ids: Vec<String> = state.results.iter()
        .filter(|(_, v)| {
            v.get("status")
                .and_then(|s| s.as_str())
                .map(|s| s == "Failed")
                .unwrap_or(false)
        })
        .map(|(k, _)| k.clone())
        .collect();
    
    if failed_ids.is_empty() {
        return Ok(vec![]);  // 无失败 → 短路返回，零开销
    }
    
    // 根据子任务类型查询实际状态
    let still_failed = self.query_children_still_failed(&failed_ids, node_instance).await?;
    
    // 差集 = 已被重试（不再是 Failed）的子任务
    Ok(failed_ids.into_iter()
        .filter(|id| !still_failed.contains(id))
        .collect())
}
```

### 8.5 query_children_still_failed（支持混合子任务类型）

容器内的子任务可能混合 HTTP/gRPC（存在 task_instances 集合）和 SubWorkflow（存在 workflow_instances 集合）。需要根据 child_id 判断类型后分别查询：

```rust
async fn query_children_still_failed(
    &self,
    failed_ids: &[String],
    node_instance: &WorkflowNodeInstanceEntity,
) -> anyhow::Result<HashSet<String>> {
    let mut still_failed = HashSet::new();
    
    // 按子任务类型分组
    let (task_ids, workflow_ids) = self.classify_child_ids(failed_ids, node_instance);
    
    // 查 TaskInstance（HTTP/gRPC 子任务）
    if !task_ids.is_empty() {
        let failed_tasks = self.task_instance_svc
            .find_by_ids_with_status(&task_ids, TaskInstanceStatus::Failed)
            .await?;
        still_failed.extend(failed_tasks.into_iter().map(|t| t.task_instance_id));
    }
    
    // 查 WorkflowInstance（SubWorkflow 子任务）
    if !workflow_ids.is_empty() {
        let failed_workflows = self.workflow_instance_svc
            .find_by_ids_with_status(&workflow_ids, WorkflowInstanceStatus::Failed)
            .await?;
        still_failed.extend(failed_workflows.into_iter().map(|w| w.workflow_instance_id));
    }
    
    Ok(still_failed)
}
```

**子任务类型判定**：根据容器的 `task_template` 配置（Parallel 的内部模板类型、ForkJoin 的 tasks 列表中每项的 task_template 类型）可确定每个 child_id 对应的实体类型。或者在 `results` map 中记录子任务类型，简化运行时判定。

### 8.6 reconcile_state

```rust
fn reconcile_state(
    &self,
    state: &mut ContainerState,
    stale_ids: &[String],
) {
    for id in stale_ids {
        // 回退计数
        state.failed_count -= 1;
        // 从 processed_callbacks 移除
        state.processed_callbacks.retain(|x| x != id);
        // 重置结果
        state.results.insert(id.clone(), serde_json::Value::Null);
    }
}
```

### 8.7 性能分析

| 场景 | failed_ids 数量 | 查询代价 | 说明 |
|------|----------------|---------|------|
| 所有子任务成功（常见） | 0 | 零（短路） | `failed_ids.is_empty()` 直接返回 |
| 有 5 个失败，无重试 | 5 | 1-2 次查询（按类型分组），每次 5 条 | 轻量 |
| 有 5 个失败，2 个被重试 | 5 | 同上 | 轻量 |
| 高失败率（100/10000 失败） | 100 | 1-2 次查询，每次 100 条 | 仍可接受 |

**结论**：查询规模 = `failed_count`（非 `total_items`），在绝大多数业务场景中极小。全部成功路径零开销。

---

## 9. 状态转换规则（完整）

### 9.1 WorkflowInstance 状态机

```rust
fn validate_workflow_transition(
    old: &WorkflowInstanceStatus,
    new: &WorkflowInstanceStatus,
) -> Result<(), TransitionError> {
    use WorkflowInstanceStatus::*;
    let valid = match (old, new) {
        // 正向推进
        (Pending, Running) => true,
        (Running, Await) => true,
        (Running, Suspended) => true,
        (Running, Completed) => true,
        (Running, Failed) => true,
        (Running, Canceled) => true,
        
        // 唤醒 / 恢复
        (Await, Pending) => true,
        (Await, Canceled) => true,
        (Suspended, Pending) => true,
        (Suspended, Canceled) => true,
        
        // 重试（人工或 ChildRevived 触发）
        (Failed, Pending) => true,
        (Failed, Canceled) => true,
        
        _ => false,
    };
    
    if valid { Ok(()) }
    else { Err(TransitionError::InvalidTransition { from: old.clone(), to: new.clone() }) }
}
```

**注意：不引入 `Failed → Await`。** 所有恢复路径统一走 `Failed → Pending`，由 Worker 持锁进入 Running 后通过 plugin gather 决定是 Await 还是其他状态。`Pending` 是统一安全边界。

### 9.2 NodeExecutionStatus 状态机

节点状态遵循相同原则：

| from | to | 场景 |
|------|----|------|
| `Pending` | `Running` | 开始执行 |
| `Running` | `Success` | 执行成功 |
| `Running` | `Failed` | 执行失败 |
| `Running` | `Await` | 等待异步回调 |
| `Running` | `Suspended` | 审批/暂停 |
| `Failed` | `Pending` | ChildRevived 恢复 / 用户重试 |
| `Failed` | `Skipped` | 用户跳过 |
| `Suspended` | `Pending` | 恢复 |
| `Suspended` | `Skipped` | 用户跳过 |

---

## 10. 完整场景推演（代码级）

> 以下推演基于实际代码结构。标注格式：`文件名:行号` 或 `函数名()`。
> 状态机规则来源：`shared/workflow.rs:58-73 can_transition_to()`。

### 10.1 正常路径：独立 SubWorkflow 执行成功（对照用）

```
┌─ 父工作流执行到 SubWorkflow 节点 ─────────────────────────────────────────────┐
│                                                                                 │
│ [入口] process_workflow_job(Start)                                               │
│   workflow.rs:44 → acquire_lock(父, worker_id, 10000ms)                         │
│   workflow.rs:71 → on_workflow_start()                                          │
│     workflow.rs:136 → execute_workflow()                                        │
│       workflow.rs:328 → start_instance() 📌 父: Pending→Running                │
│       workflow.rs:346 → execute_workflow_loop()                                 │
│         workflow.rs:374 → workflow_loop_tick()                                  │
│           workflow.rs:460 → 当前节点 status == Pending                           │
│             workflow.rs:461 📌 node.status = Running                             │
│             workflow.rs:464 → run_node()                                        │
│               workflow.rs:521 → execute_node_instance()                         │
│                 → SubWorkflowPlugin::execute()  [subworkflow.rs:29-124]         │
│                   subworkflow.rs:88 → create_instance(子, parent_ctx)           │
│                   subworkflow.rs:117-123 → return async_dispatch_workflow(job)  │
│                     job = ExecuteWorkflowJob { 子ID, Start }                    │
│                     interface.rs:48-49 → status = NodeExecutionStatus::Await    │
│               workflow.rs:545 → apply_exec_result()                             │
│                 apply_exec.rs:24 📌 node.status = Await                         │
│                 apply_exec.rs:43-44 📌 instance.status = Await                  │
│                 apply_exec.rs:54 📌 save_instance_and_bump_epoch() CAS写入       │
│                 apply_exec.rs:70-78 → dispatch_workflow(子的Start job)           │
│                                                                                 │
│   workflow.rs:94-101 → notify_parent_if_needed(父ID)                           │
│     workflow.rs:555-558 → 父是 Await (非终态) → return Ok(()) 不通知            │
│   workflow.rs:104-114 → release_lock(父)                                       │
│                                                                                 │
│ [结果] 父: Await, SubWorkflow节点: Await                                        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 子工作流执行完毕 ─────────────────────────────────────────────────────────────┐
│                                                                                 │
│ [入口] process_workflow_job(Start) — 子实例                                      │
│   ... 子的各节点正常执行 ...                                                     │
│   最后节点 Success + 无 next_node:                                               │
│     workflow.rs:440-443 → complete_instance() 📌 子: Running→Completed          │
│                                                                                 │
│   workflow.rs:94-101 → notify_parent_if_needed(子ID)                           │
│     workflow.rs:555 → is_terminal = true (Completed) ✓                         │
│     workflow.rs:564 → parent_context 非空 ✓                                     │
│     workflow.rs:575-588 📌 dispatch_workflow(NodeCallback 给父)                  │
│       event = NodeCallback {                                                    │
│         node_id: 父的SubWorkflow节点ID,                                         │
│         child_task_id: 子工作流实例ID,                                           │
│         status: Success,                                                        │
│         output: Some(子.context)                                                │
│       }                                                                         │
│   release_lock(子)                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 父收到子成功回调 ─────────────────────────────────────────────────────────────┐
│                                                                                 │
│ [入口] process_workflow_job(NodeCallback{Success})                               │
│   acquire_lock(父)                                                              │
│   workflow.rs:80-91 → on_node_callback()                                       │
│     workflow.rs:150 → prepare_instance_for_node_callback()                     │
│       workflow.rs:223-237 → status == Await:                                   │
│         📌 wake_from_await() → Await→Pending                                   │
│         📌 start_instance() → Pending→Running                                  │
│         reload instance                                                         │
│         return Ready(instance)                                                  │
│     workflow.rs:183-205 → handle_node_callback()                               │
│       → SubWorkflow 无自定义 handle_callback                                    │
│       → interface.rs:63-86 默认实现:                                            │
│         status == Success → return ExecutionResult::success(None)               │
│     workflow.rs:208 → apply_exec_result()                                      │
│       📌 node.status = Success                                                  │
│       有 next_node → instance.current_node = next → LoopAction::Advance        │
│       (或无 next_node → instance.status = Completed → LoopAction::Done)         │
│       save CAS                                                                  │
│     workflow.rs:210-211 → Advance → execute_workflow_loop() 继续               │
│                                                                                 │
│   notify_parent_if_needed(父) → 根工作流无 parent_context → 不通知             │
│   release_lock(父)                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 BUG 路径：子工作流失败后重试 — 当前实现（回调被丢弃）

```
┌─ T1: 子工作流执行失败 ─────────────────────────────────────────────────────────┐
│                                                                                 │
│ 子执行中某节点失败:                                                               │
│   workflow_loop_tick:447-458                                                    │
│   workflow.rs:453-456 → fail_instance() 📌 子: Running→Failed                  │
│                                                                                 │
│   notify_parent_if_needed(子):                                                  │
│     is_terminal(Failed) = true ✓                                               │
│     parent_context 非空 ✓                                                       │
│     📌 dispatch NodeCallback(Failed) 给父                                       │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T2: 父收到子失败回调 ─────────────────────────────────────────────────────────┐
│                                                                                 │
│ process_workflow_job(NodeCallback{Failed})                                       │
│   acquire_lock(父)                                                              │
│   on_node_callback():                                                           │
│     prepare_instance_for_node_callback():                                       │
│       父是 Await → wake_from_await + start_instance                            │
│       📌 父: Await→Pending→Running                                              │
│     handle_node_callback() → 默认实现 → ExecutionResult::failed()              │
│     apply_exec_result():                                                        │
│       📌 apply_exec.rs:24 → node.status = Failed                               │
│       📌 apply_exec.rs:39-40 → instance.status = Failed                        │
│       save CAS                                                                  │
│                                                                                 │
│   notify_parent_if_needed(父):                                                  │
│     父 Failed, 是根工作流(无 parent_context) → 不通知                           │
│   release_lock(父)                                                              │
│                                                                                 │
│ [结果] 父: Failed, 节点: Failed                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T3: 用户重试子工作流内部节点 ─────────────────────────────────────────────────┐
│                                                                                 │
│ API: POST /workflow/instances/{子ID}/retry-node { node_id: "http_3" }           │
│                                                                                 │
│ handler → retry_workflow_node(子ID, "http_3", None):                            │
│   service.rs:553 → SubWorkflow 检查 PASS（对子实例来说它自己不是SubWorkflow节点）│
│   service.rs:652 → 原子节点重试路径:                                             │
│     service.rs:672 📌 子.nodes[idx].status = Pending                            │
│     service.rs:678 📌 子.status = Pending  ← Failed→Pending                    │
│     service.rs:681 📌 save                                                      │
│                                                                                 │
│   handler:215-222 📌 dispatch_workflow(Start) 给子                              │
│                                                                                 │
│ ❌ 注意：子 Failed→Pending 转换发生了                                            │
│ ❌ 但没有任何代码通知父工作流                                                     │
│ ❌ 父仍然是 Failed 状态                                                          │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T4: 子重新执行并成功 ─────────────────────────────────────────────────────────┐
│                                                                                 │
│ Worker 消费子的 Start → 子: Pending→Running → ... → Completed                   │
│                                                                                 │
│   notify_parent_if_needed(子):                                                  │
│     is_terminal(Completed) = true ✓                                            │
│     parent_context 非空 ✓                                                       │
│     📌 dispatch NodeCallback(Success) 给父                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T5: 父收到子成功回调 ❌ BUG ──────────────────────────────────────────────────┐
│                                                                                 │
│ process_workflow_job(NodeCallback{Success}) for 父                               │
│   acquire_lock(父)                                                              │
│   on_node_callback():                                                           │
│     prepare_instance_for_node_callback():                                       │
│       workflow.rs:222 → match instance.status:                                  │
│       workflow.rs:268-274 → status == Failed → 进入 _ 分支                      │
│         debug!("node callback ignored: instance not in await/suspended/...")    │
│         📌 return CallbackReadiness::Ignored                                    │
│   workflow.rs:152 → CallbackReadiness::Ignored → return Ok(())                 │
│                                                                                 │
│   notify_parent_if_needed → 父仍 Failed 但已处理完 → 不重复通知                 │
│   release_lock(父)                                                              │
│                                                                                 │
│ ❌❌❌ 回调被丢弃！父永远停在 Failed！❌❌❌                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.3 方案 D 修正路径：子工作流重试 → 正确恢复父

```
┌─ T3 (修正): 用户重试子工作流内部节点 ─────────────────────────────────────────┐
│                                                                                 │
│ API: POST /workflow/instances/{子ID}/retry-node { node_id: "http_3" }           │
│                                                                                 │
│ handler → retry_workflow_node(子ID, "http_3", None):                            │
│   service.rs:672 📌 子.nodes[idx].status = Pending                              │
│                                                                                 │
│   🆕 service.rs:678 (改造) → 子.transition_status(Pending)                     │
│     ├─ validate: can_transition_to(Failed, Pending) ✓ (shared/workflow.rs:66)  │
│     ├─ 子.status = Pending                                                     │
│     ├─ should_notify_parent(Failed, Pending) → Some(Revived)                  │
│     ├─ 子.parent_context == Some(父ctx) ✓                                      │
│     └─ return StateTransitionResult {                                          │
│          outbound: [OutboundEvent {                                             │
│            target: 父工作流ID,                                                   │
│            event: ChildRevived { node_id: "subwf_node", child_id: 子ID }       │
│          }]                                                                     │
│        }                                                                        │
│                                                                                 │
│   service.rs:681 📌 save 子工作流                                                │
│                                                                                 │
│   🆕 投递出站事件:                                                               │
│     📌 dispatch_workflow(ExecuteWorkflowJob {                                   │
│         workflow_instance_id: 父工作流ID,                                        │
│         tenant_id: ...,                                                         │
│         event: ChildRevived { node_id: "subwf_node", child_id: 子ID }          │
│     })                                                                          │
│                                                                                 │
│   handler:215-222 📌 dispatch_workflow(Start) 给子                              │
│                                                                                 │
│ [两个 Job 入队: 1.父的ChildRevived  2.子的Start]                                │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T4: 父收到 ChildRevived ─────────────────────────────────────────────────────┐
│                                                                                 │
│ [入口] process_workflow_job(ChildRevived{node_id, child_id})                     │
│   workflow.rs:44 → acquire_lock(父)                                             │
│   🆕 workflow.rs 新增分支 → on_child_revived()                                  │
│                                                                                 │
│   on_child_revived():                                                           │
│     父.status == Failed → revive_from_failed():                                │
│       node = find_node("subwf_node")                                           │
│       node.node_type == SubWorkflow:                                            │
│         📌 node.status = Pending (Failed→Pending)                               │
│         📌 父.status = Pending (Failed→Pending)                                 │
│           validate: can_transition_to(Failed, Pending) ✓ (shared:66)           │
│       📌 save_instance_and_bump_epoch() CAS写入                                 │
│       📌 dispatch_workflow(ExecuteWorkflowJob {                                 │
│           workflow_instance_id: 父ID,                                           │
│           event: Start                                                          │
│       })                                                                        │
│                                                                                 │
│   release_lock(父)                                                              │
│                                                                                 │
│ [结果] 父: Pending, SubWorkflow节点: Pending                                    │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T5: Worker 拉起父工作流 (Start) ─────────────────────────────────────────────┐
│                                                                                 │
│ [入口] process_workflow_job(Start) — 父实例                                      │
│   acquire_lock(父)                                                              │
│   on_workflow_start():                                                          │
│     workflow.rs:124 → is_pending() == true ✓                                   │
│     execute_workflow():                                                          │
│       workflow.rs:327-328 → start_instance()                                   │
│         📌 父: Pending→Running (validate ✓ shared:61)                           │
│       execute_workflow_loop():                                                  │
│         workflow_loop_tick():                                                    │
│           current_node = "subwf_node" (current_node 未变)                       │
│           node.status == Pending (在 T4 中被设为 Pending)                       │
│           workflow.rs:460-461 📌 node.status = Running                           │
│           run_node() → execute_node_instance()                                 │
│             → SubWorkflowPlugin::execute()                                     │
│                                                                                 │
│               🆕 Re-evaluation 路径:                                            │
│               读取 node_instance.task_instance.output                           │
│                 → {"child_workflow_instance_id": "子ID"}                        │
│               查询子工作流实例状态:                                               │
│                 instance_svc.get_workflow_instance(子ID)                         │
│                 子.status == Running（子正在重试执行中）                           │
│               📌 return ExecutionResult::async_dispatch_workflow(空)             │
│                 → 或者特定的 await_only 变体:                                    │
│                   ExecutionResult { status: Await, dispatch_jobs: [], ... }     │
│                                                                                 │
│           apply_exec_result():                                                  │
│             📌 apply_exec.rs:24 → node.status = Await                          │
│             📌 apply_exec.rs:43-44 → instance.status = Await                   │
│             📌 save CAS                                                         │
│             (无 dispatch jobs → 无新投递)                                       │
│                                                                                 │
│   notify_parent_if_needed(父):                                                  │
│     父是 Await (非终态) → 不通知                                                │
│   release_lock(父)                                                              │
│                                                                                 │
│ [结果] 父: Await, SubWorkflow节点: Await — 正确等待子完成                        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T6: 子工作流重试成功 ─────────────────────────────────────────────────────────┐
│                                                                                 │
│ 子执行: Pending→Running → ... → 所有节点 Success → 无 next_node                 │
│   workflow.rs:440-443 → complete_instance()                                    │
│   📌 子: Running→Completed                                                      │
│                                                                                 │
│   notify_parent_if_needed(子):                                                  │
│     is_terminal(Completed) = true ✓                                            │
│     parent_context 非空 ✓                                                       │
│     📌 dispatch NodeCallback(Success) { node_id: "subwf_node", child_task_id: 子ID }│
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T7: 父收到子成功回调 ✅ 正确处理 ─────────────────────────────────────────────┐
│                                                                                 │
│ process_workflow_job(NodeCallback{Success})                                      │
│   acquire_lock(父)                                                              │
│   on_node_callback():                                                           │
│     prepare_instance_for_node_callback():                                       │
│       workflow.rs:223-237 → status == Await ✅ (不再是Failed!)                  │
│         wake_from_await() 📌 Await→Pending (validate ✓ shared:71)              │
│         start_instance() 📌 Pending→Running                                    │
│         reload, return Ready                                                    │
│     handle_node_callback():                                                     │
│       → 默认实现 → status Success → ExecutionResult::success(None)             │
│     apply_exec_result():                                                        │
│       📌 node.status = Success                                                  │
│       推进 current_node = next_node (或 Completed)                              │
│       save CAS                                                                  │
│     execute_workflow_loop() 继续                                                │
│                                                                                 │
│   notify_parent_if_needed(父) → 根工作流 → 不通知                               │
│   release_lock(父)                                                              │
│                                                                                 │
│ ✅ 父工作流正确恢复并推进！                                                      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.4 快速路径：子已完成时父的 Re-evaluation

```
如果 T5 发生在子工作流已经完成之后（子先完成，Revived后处理）:

T5 (快速路径): Worker 拉起父
  → SubWorkflowPlugin::execute() re-evaluation
  → 查子状态 → 子是 Completed!
  → 📌 return ExecutionResult::success(子.context)
  → apply_exec_result: node.status = Success, 推进
  → 父直接继续，不需要等回调

此时 T6 的 NodeCallback(Success) 到达父时:
  prepare_instance_for_node_callback:
    父可能已经不在 Await 了（已推进到下一节点或已 Completed）
    → 如果已不在可接受状态 → Ignored (幂等安全)
```

### 10.5 竞争窗口（Parallel 容器）：Stale Check 消除误判

```
Parallel(total=100, max_failures=None)
已处理回调: 98 成功 + SubWorkflow-99 失败 = 99个回调
状态机: { success_count:98, failed_count:1, total:100 }
父: Await (子99失败但未触发 max_failures)

┌─ T1: 用户重试子工作流-99 ──────────────────────────────────────────────────────┐
│                                                                                 │
│ 子-99: Failed→Pending (transition_status → Revived 入队)                        │
│ [队列中: Revived for 父]                                                        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T2: 第100个HTTP任务完成（先于Revived被处理） ──────────────────────────────────┐
│                                                                                 │
│ process_workflow_job(NodeCallback{Success, child_task_id="xxx-node-99"})         │
│   prepare_instance_for_node_callback: 父是 Await → wake + start → Running     │
│   handle_node_callback → ParallelPlugin::handle_callback():                    │
│                                                                                 │
│     1. 正常处理: success_count: 98→99                                           │
│                                                                                 │
│     2. 🆕 Stale Failure Check:                                                 │
│        failed_ids = ["子-99-实例ID"] (results中 status=="Failed" 的)            │
│        query_children_still_failed(["子-99-实例ID"]):                           │
│          子-99 实际状态 = Pending (已被重试!) → 不在 still_failed 中            │
│        stale_ids = ["子-99-实例ID"]                                             │
│                                                                                 │
│     3. reconcile_state:                                                         │
│        failed_count: 1→0                                                        │
│        processed_callbacks 移除 "子-99-实例ID"                                  │
│        results["子-99-实例ID"] = null                                           │
│                                                                                 │
│     4. evaluate_completion:                                                     │
│        success_count(99) + failed_count(0) = 99 ≠ 100                          │
│        → 未全部完成 → return async_dispatch_multiple([])                        │
│        → node.status = Await, instance.status = Await                          │
│                                                                                 │
│ ✅ 没有误判为 all_done！继续等子-99的新回调                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T3: Revived 到达父 ──────────────────────────────────────────────────────────┐
│                                                                                 │
│ process_workflow_job(ChildRevived) for 父                                       │
│   on_child_revived():                                                           │
│     父.status == Await → revive_from_await():                                  │
│       rollback_child_in_container:                                              │
│         "子-99-实例ID" 不在 processed_callbacks 中（T2已移除）→ 幂等跳过        │
│       保持 Await 状态                                                           │
│                                                                                 │
│ ✅ 幂等无害                                                                     │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ T4: 子-99 重试成功 → NodeCallback(Success) 给父 ──────────────────────────────┐
│                                                                                 │
│ Parallel handle_callback:                                                       │
│   "子-99-实例ID" 不在 processed_callbacks 中 → 非重复                           │
│   success_count: 99→100                                                         │
│   stale check: failed_ids=[] → 短路返回                                         │
│   evaluate: 100+0 == 100, failed_count==0 → ✅ Success!                        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.6 独立 HTTP/gRPC 节点失败后重试（无子工作流参与）

```
父工作流图: Node1(Http,成功) → Node2(SubWorkflow,成功) → Node3(Http,失败)
父: Failed, current_node = "Node3"

这里没有子工作流要通知 — 是父自己的原子节点失败。
```

```
┌─ 用户重试 Node3 (retry-node API) ─────────────────────────────────────────────┐
│                                                                                 │
│ API: POST /workflow/instances/{父ID}/retry-node { node_id: "Node3" }            │
│                                                                                 │
│ handler → retry_workflow_node(父ID, "Node3", None):                             │
│   service.rs:553 → Node3.node_type == Http → SubWorkflow 检查 PASS             │
│   service.rs:548-551 → is_container = false                                    │
│   service.rs:652 → 原子节点重试路径:                                             │
│     service.rs:659 → node.status == Failed ✓                                   │
│     service.rs:667-668 → task_instance_svc.retry_instance(task_id) → Pending   │
│     service.rs:672 📌 node.status = Pending                                     │
│     service.rs:678 📌 inst.status = Pending  ← Failed→Pending                  │
│       (validate: can_transition_to(Failed, Pending) ✓ shared:66)               │
│     service.rs:681 📌 save                                                      │
│                                                                                 │
│   🆕 transition_status 检查:                                                    │
│     父.parent_context == None (这是根工作流)                                     │
│     → 无出站事件 (无人需要通知)                                                   │
│                                                                                 │
│ handler:215-222 📌 dispatch_workflow(Start) 给父自身                             │
│                                                                                 │
│ [结果] 父: Pending, Node3: Pending                                              │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ Worker 拉起父工作流 (Start) ──────────────────────────────────────────────────┐
│                                                                                 │
│ process_workflow_job(Start) — 父实例                                             │
│   acquire_lock(父)                                                              │
│   on_workflow_start():                                                          │
│     execute_workflow():                                                          │
│       start_instance() 📌 父: Pending→Running                                  │
│       execute_workflow_loop():                                                  │
│         workflow_loop_tick():                                                    │
│           current_node = "Node3" (重试时 current_node 不变)                      │
│           node.status == Pending → 进入执行                                     │
│           📌 node.status = Running                                              │
│           run_node() → HttpPlugin::execute()                                   │
│             → 创建 ExecuteTaskJob(task_instance_id)                             │
│             → return async_dispatch(job)                                        │
│               interface.rs:40-41 → status = Await                              │
│           apply_exec_result():                                                  │
│             📌 node.status = Await                                              │
│             📌 instance.status = Await                                          │
│             save CAS                                                            │
│             dispatch_task(job)                                                  │
│                                                                                 │
│   release_lock(父)                                                              │
│                                                                                 │
│ [结果] 父: Await, Node3: Await, HTTP任务已投递给 Task Worker                     │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ Task Worker 执行 HTTP 任务成功 → 回调父 ─────────────────────────────────────┐
│                                                                                 │
│ Task Worker 完成 → dispatch NodeCallback(Success) 给父                          │
│                                                                                 │
│ process_workflow_job(NodeCallback{Success, node_id:"Node3"})                    │
│   prepare_instance_for_node_callback: 父是 Await → wake + start → Running     │
│   handle_node_callback: 默认实现 → Success                                     │
│   apply_exec_result:                                                            │
│     node.status = Success                                                       │
│     Node3 无 next_node → instance.status = Completed                           │
│     save CAS                                                                    │
│                                                                                 │
│ [结果] 父: Completed ✅                                                         │
└─────────────────────────────────────────────────────────────────────────────────┘

[分析] 独立 HTTP/gRPC 节点重试:
  - 不涉及 ChildRevived（父自己就是被重试的实例）
  - 若父是根工作流 → parent_context == None → 无出站事件
  - 若父本身是某个更上层工作流的子 → parent_context != None
    → transition_status(Failed→Pending) → Revived 通知祖父
    → 祖父也会走 revive_from_failed 路径
    → 递归向上传播，直到根工作流
```

### 10.7 嵌套场景：祖父 → 父(SubWorkflow) → 子(SubWorkflow)，子失败后重试

```
祖父工作流: GrandNode1(Http) → GrandNode2(SubWorkflow→父工作流)
父工作流:   ParentNode1(Http) → ParentNode2(SubWorkflow→子工作流)
子工作流:   ChildNode1(Http,失败)

失败传播: 子Failed → 父Failed → 祖父Failed
```

```
┌─ 失败传播（当前已正确实现） ────────────────────────────────────────────────────┐
│                                                                                 │
│ 1. 子: ChildNode1 Failed → 子 Failed                                           │
│    notify_parent_if_needed → NodeCallback(Failed) 给父                          │
│                                                                                 │
│ 2. 父收到: prepare(Await→Running) → handle_callback → Failed                  │
│    apply_exec_result → ParentNode2 Failed → 父 Failed                          │
│    notify_parent_if_needed → NodeCallback(Failed) 给祖父                        │
│                                                                                 │
│ 3. 祖父收到: prepare(Await→Running) → handle_callback → Failed                │
│    apply_exec_result → GrandNode2 Failed → 祖父 Failed                         │
│    notify_parent_if_needed → 祖父是根(无parent) → 不通知                        │
│                                                                                 │
│ [结果] 子:Failed, 父:Failed, 祖父:Failed                                       │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 用户重试子工作流 ChildNode1 (方案D) ──────────────────────────────────────────┐
│                                                                                 │
│ API: retry-node(子ID, "ChildNode1")                                             │
│                                                                                 │
│ retry_workflow_node:                                                             │
│   🆕 子.transition_status(Pending):                                             │
│     子.parent_context = Some(父ctx) → should_notify(Failed,Pending) → Revived │
│     outbound = [ChildRevived → 父]                                             │
│   save 子                                                                       │
│   📌 dispatch ChildRevived 给父                                                 │
│   📌 dispatch Start 给子                                                        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 父收到 ChildRevived ─────────────────────────────────────────────────────────┐
│                                                                                 │
│ on_child_revived():                                                             │
│   父.status == Failed → revive_from_failed():                                  │
│     ParentNode2: Failed→Pending                                                │
│     🆕 父.transition_status(Pending):                                           │
│       父.parent_context = Some(祖父ctx) → should_notify(Failed,Pending) → Revived│
│       outbound = [ChildRevived → 祖父]                                         │
│     save 父                                                                     │
│     📌 dispatch ChildRevived 给祖父                                             │
│     📌 dispatch Start 给父                                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 祖父收到 ChildRevived ───────────────────────────────────────────────────────┐
│                                                                                 │
│ on_child_revived():                                                             │
│   祖父.status == Failed → revive_from_failed():                                │
│     GrandNode2: Failed→Pending                                                 │
│     🆕 祖父.transition_status(Pending):                                         │
│       祖父.parent_context = None → 无出站事件                                   │
│     save 祖父                                                                   │
│     📌 dispatch Start 给祖父                                                    │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 三层 Worker 拉起 ────────────────────────────────────────────────────────────┐
│                                                                                 │
│ 祖父 Start:                                                                     │
│   Pending→Running → execute_loop → GrandNode2 Pending                          │
│   SubWorkflowPlugin::execute() re-evaluation:                                  │
│     查父状态 → 父是 Pending/Running → return await_callback()                  │
│   祖父: Running→Await                                                           │
│                                                                                 │
│ 父 Start:                                                                       │
│   Pending→Running → execute_loop → ParentNode2 Pending                         │
│   SubWorkflowPlugin::execute() re-evaluation:                                  │
│     查子状态 → 子是 Pending/Running → return await_callback()                  │
│   父: Running→Await                                                             │
│                                                                                 │
│ 子 Start:                                                                       │
│   Pending→Running → execute_loop → ChildNode1 Pending                          │
│   HttpPlugin::execute() → dispatch_task                                        │
│   子: Running→Await                                                             │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 子成功 → 逐层回调 ───────────────────────────────────────────────────────────┐
│                                                                                 │
│ Task完成 → 子收到callback → ChildNode1 Success → 子Completed                   │
│   notify_parent → NodeCallback(Success) 给父                                   │
│                                                                                 │
│ 父收到: prepare(Await→Running) → handle → ParentNode2 Success                 │
│   → 父Completed                                                                │
│   notify_parent → NodeCallback(Success) 给祖父                                 │
│                                                                                 │
│ 祖父收到: prepare(Await→Running) → handle → GrandNode2 Success                │
│   → 祖父Completed ✅                                                           │
│                                                                                 │
│ [结果] 三层全部恢复完成 ✅                                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.8 对照：容器内 HTTP 子任务重试（现有实现 vs 方案 D）

```
┌─ 现有实现（API 直接 rollback + dispatch_task） ────────────────────────────────┐
│                                                                                 │
│ API: retry-node(父ID, container_node, child_task_id="xxx-node-5")              │
│                                                                                 │
│ retry_workflow_node() 容器路径 [service.rs:559-648]:                             │
│   service.rs:609 → processed_callbacks 移除                                    │
│   service.rs:614 → failed_count -= 1                                           │
│   service.rs:619 → results[cid] = null                                         │
│   service.rs:624 → task_instance_svc.retry_instance(cid) → task Failed→Pending │
│   service.rs:636-638 → 📌 父: Failed→Await                                    │
│     (validate: can_transition_to(Failed, Await) ✓ shared:68)                   │
│   service.rs:640 → save                                                        │
│                                                                                 │
│ handler:189-212 → dispatch_task(cid) 给 Task Worker                            │
│                                                                                 │
│ 后续: Task Worker 执行完 → NodeCallback 给父                                    │
│   父是 Await → prepare_instance_for_node_callback 接受 ✓                       │
│   Parallel handle_callback 正常累加                                             │
│                                                                                 │
│ [分析] 这个路径当前是完整的（虽然 Failed→Await 略显非正统）。                     │
│ 方案 D 中可保留此快速路径不变（因为 API 层已内部做了 rollback + dispatch），     │
│ 或统一改为:                                                                      │
│   - API: rollback counters + 父: Failed→Pending + dispatch Start for 父        │
│   - 父 re-evaluation → 发现已有任务在跑 → Await                                │
│   - 同时 dispatch_task(cid)                                                     │
│ 两种最终结果一致，渐进迁移可先保留现有逻辑。                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.9 ForkJoin 容器内 SubWorkflow 子任务失败后重试

```
当前架构约束（关键）:
  - Parallel: items_path 驱动，所有子任务共享同一 task_template
    → 子任务只能是同一类型（HTTP/gRPC/LLM）
    → 当前不支持 SubWorkflow 子任务（items 是数据数组）
  - ForkJoin: tasks 列表驱动，每个 ForkJoinTaskItem 有独立 task_template
    → task_template 类型是 TaskTemplate 枚举 → 可以包含 SubWorkflow
    → 但当前 execute() 只 dispatch ExecuteTaskJob → Task Worker 无 SubWorkflow executor
    → ❌ 目前实际上不支持 SubWorkflow 子任务，但数据结构允许

本方案需在 Phase 2 中让 ForkJoin 支持混合子任务类型（SubWorkflow + HTTP/gRPC）。
以下推演基于 Phase 2 实现后的行为。
```

```
ForkJoin(tasks=[
  { task_key:"create_user", task_template: Http(...) },     // index 0
  { task_key:"send_email",  task_template: Http(...) },     // index 1
  { task_key:"sync_crm",   task_template: SubWorkflow(...) }, // index 2
])

各子任务 dispatch:
  - create_user(0), send_email(1) → dispatch_task → Task Worker
  - sync_crm(2) → dispatch_workflow → 子工作流 Worker (Phase 2 改造)
父: Await

场景: sync_crm(子工作流) 失败 → max_failures 触发 → ForkJoin Failed → 父 Failed
```

```
┌─ 用户通过子工作流实例重试（路径 B） ───────────────────────────────────────────┐
│                                                                                 │
│ ❓ 两种重试入口:                                                                │
│                                                                                 │
│ 路径 A: 父容器 retry-node API (child_task_id=子工作流实例ID)                     │
│   → 需要 service 层识别 child_task_id 是 WorkflowInstance 而非 TaskInstance     │
│   → 需要新增: workflow_instance_svc.transition_status(子, Pending)              │
│   → 复杂，Phase 2+                                                              │
│                                                                                 │
│ 路径 B: 直接对子工作流实例调 retry-node                                          │
│   → 与 10.3 完全相同的路径                                                       │
│   → transition_status 自动产生 Revived，parent_context 指向父容器节点           │
│   → 无需容器感知，通用性强                                                       │
│                                                                                 │
│ Phase 1 选择路径 B（通用）                                                       │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 用户重试 sync_crm 子工作流内部节点 ──────────────────────────────────────────┐
│                                                                                 │
│ API: retry-node(sync_crm子工作流实例ID, "内部http节点")                          │
│                                                                                 │
│ retry_workflow_node(sync_crm实例):                                              │
│   sync_crm.nodes[idx].status = Pending                                         │
│   🆕 sync_crm.transition_status(Pending):                                      │
│     sync_crm.parent_context = Some({                                           │
│       workflow_instance_id: 父ID,                                               │
│       node_id: "forkjoin_node",                                                │
│       parent_task_instance_id: None,                                           │
│       item_index: Some(2)                                                      │
│     })                                                                          │
│     should_notify(Failed, Pending) → Revived                                  │
│     outbound = [ChildRevived {                                                 │
│       node_id: "forkjoin_node",                                                │
│       child_id: sync_crm.workflow_instance_id                                  │
│     }]                                                                          │
│   save sync_crm                                                                 │
│   📌 dispatch ChildRevived 给父                                                 │
│   📌 dispatch Start 给 sync_crm                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ 父收到 ChildRevived { node_id:"forkjoin_node", child_id:sync_crm_id } ───────┐
│                                                                                 │
│ on_child_revived():                                                             │
│   父.status == Failed → revive_from_failed():                                  │
│     find_node("forkjoin_node") → node.node_type == ForkJoin                   │
│                                                                                 │
│     📌 rollback_child_in_container("forkjoin_node", sync_crm_id):             │
│       state = node.task_instance.output                                        │
│       ForkJoin 的 processed_callbacks 使用 child_task_id 格式:                 │
│         "{wf_id}-{node_id}-{index}" = "xxx-forkjoin_node-2"                   │
│       但子工作流回调时 child_task_id = workflow_instance_id（非此格式）          │
│       → results 中需通过 task_key 索引 (见 ForkJoin resolve_task_key)           │
│                                                                                 │
│       方案: 使用 child_id 在 processed_callbacks 中查找                         │
│         (子工作流 callback 时 child_task_id = sync_crm.workflow_instance_id)   │
│         processed_callbacks 移除 sync_crm.workflow_instance_id                 │
│       results: resolve task_key "sync_crm" → results["sync_crm"].status==Failed│
│         → results["sync_crm"] = null                                           │
│       failed_count -= 1                                                        │
│                                                                                 │
│     📌 node.status = Pending (Failed→Pending)                                  │
│     📌 父.transition_status(Pending):                                           │
│       父.parent_context = None → 无出站                                         │
│     save + dispatch Start 给父                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ Worker 拉起父 (Start) ───────────────────────────────────────────────────────┐
│                                                                                 │
│ execute_workflow():                                                              │
│   Pending→Running                                                               │
│   execute_loop → forkjoin_node Pending                                         │
│   📌 node.status = Running                                                      │
│   ForkJoinPlugin::execute() 🆕 re-evaluation:                                  │
│     state.dispatched_count > 0 → 进入 gather 模式                              │
│     查实际子任务状态 (根据 results 中的 type 字段区分查询目标):                   │
│       "create_user" (type=task): task_instance → Completed ✓                   │
│       "send_email" (type=task): task_instance → Completed ✓                    │
│       "sync_crm" (type=workflow): workflow_instance → Running (正在重试)        │
│     actual_completed = 2, actual_running = 1, actual_failed = 0                │
│     2 + 0 ≠ 3 → 未全部完成                                                    │
│     → return await_callback()                                                  │
│                                                                                 │
│   apply_exec_result:                                                            │
│     node.status = Await, instance.status = Await                               │
│     save CAS                                                                    │
│                                                                                 │
│ [结果] 父: Await — 正确等待 sync_crm 完成                                       │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─ sync_crm 重试成功 → 回调父 ──────────────────────────────────────────────────┐
│                                                                                 │
│ sync_crm: Running→Completed                                                     │
│   notify_parent_if_needed:                                                      │
│     parent_context.node_id = "forkjoin_node"                                   │
│     📌 dispatch NodeCallback(Success) {                                         │
│       node_id: "forkjoin_node",                                                │
│       child_task_id: sync_crm.workflow_instance_id,                            │
│       status: Success, output: sync_crm.context                                │
│     }                                                                           │
│                                                                                 │
│ 父 ForkJoin handle_callback:                                                    │
│   child_task_id = sync_crm.workflow_instance_id                                │
│   resolve_task_key: 需要匹配（Phase 2 改造 resolve 逻辑）                      │
│   success_count += 1                                                            │
│   Stale Check: failed_ids = [] → 短路                                           │
│   evaluate: success(3) + failed(0) == 3 → ✅ all done, Success!               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```
⚠️ ForkJoin 支持混合子任务的改造要点:

1. execute() 中根据 ForkJoinTaskItem.task_template 类型区分:
   - Http/Grpc/Llm → ExecuteTaskJob (现有)
   - SubWorkflow → 创建子工作流实例 + ExecuteWorkflowJob
   → 需要 ExecutionResult 同时返回 dispatch_jobs + dispatch_workflow_jobs

2. state.results 中记录 type 字段:
   - { "task_key": "sync_crm", "type": "workflow", "child_id": "wf-instance-id" }
   - { "task_key": "send_email", "type": "task", "child_id": "xxx-node-1" }

3. handle_callback 中 resolve_task_key 改造:
   - 现有逻辑: child_task_id 后缀 → index → task_key
   - 子工作流: child_task_id = workflow_instance_id → 需在 results 中反查

4. Stale Failure Check 中根据 type 字段选择查询 service:
   - type == "task" → task_instance_svc.get(child_id)
   - type == "workflow" → workflow_instance_svc.get(child_id)
```

---

## 11. 容器内 SubWorkflow 子任务的身份标识

### 11.1 问题

容器（Parallel/ForkJoin）的子任务通过 `child_task_id` 标识。当前格式：
- HTTP/gRPC: `{workflow_instance_id}-{node_id}-{index}`（对应 task_instances 集合中的记录）
- SubWorkflow: 子工作流实例 ID（对应 workflow_instances 集合中的记录）

两种 ID 命名空间不同，`check_stale_failures` 需要能区分它们。

### 11.2 解决方案

在容器状态机的 `results` map 中，为每个子任务记录类型信息：

```json
{
  "results": {
    "wf-node6-0": { "status": "Success", "type": "task", "output": {...} },
    "wf-node6-1": { "status": "Failed", "type": "task", "output": null },
    "child-wf-instance-id-abc": { "status": "Failed", "type": "workflow", "output": null }
  }
}
```

或者通过 ID 格式推断：
- 匹配 `{instance_id}-{node_id}-{index}` 格式 → TaskInstance
- 其他（UUID 格式的 workflow_instance_id）→ WorkflowInstance

推荐在 results 中显式记录 `type` 字段，避免格式推断的脆弱性。

### 11.3 容器派发 SubWorkflow 子任务

当 Parallel/ForkJoin 的子任务模板是 SubWorkflow 时：

```rust
// ParallelPlugin::dispatch_child() 内部
match child_template {
    TaskTemplate::SubWorkflow(sub_tmpl) => {
        // 创建子工作流实例
        let child_instance = self.workflow_instance_svc.create_instance(
            tenant_id, workflow_entity, child_context,
            Some(WorkflowCallerContext {
                workflow_instance_id: parent_instance_id,
                node_id: container_node_id,
                parent_task_instance_id: None,
                item_index: Some(index),
            }),
            depth + 1,
            created_by,
        ).await?;
        
        // 投递给 Workflow Worker（不是 Task Worker）
        dispatcher.dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: child_instance.workflow_instance_id.clone(),
            tenant_id,
            event: WorkflowEvent::Start,
        }).await?;
        
        // 记录 child_id（用于 callback 路由和 stale check）
        // child_task_id = child_instance.workflow_instance_id
    }
    TaskTemplate::Http(_) | TaskTemplate::Grpc | TaskTemplate::Llm(_) => {
        // 现有逻辑：创建 TaskInstance + dispatch_task
    }
}
```

子工作流完成后通过 `notify_parent_if_needed`（或 transition_status 自动出站）发送 NodeCallback 给父容器节点，`child_task_id` = 子工作流实例 ID。

---

## 12. 幂等性保证

### 12.1 ChildRevived 的幂等性

| 父当前状态 | 收到 ChildRevived | 行为 | 安全性 |
|-----------|-------------------|------|--------|
| Failed | `Failed → Pending` + dispatch Start | 正常处理 | ✓ |
| Await | rollback 计数（如尚未回退） | 正常处理 | ✓ |
| Pending/Running | 忽略（父已在恢复中） | warn + skip | ✓ |
| Completed | 忽略（父已完成） | warn + skip | ✓ |

### 12.2 容器 rollback 的幂等保护

```rust
// 如果 child_id 已不在 processed_callbacks 中 → 已回退过 → no-op
if !state.processed_callbacks.contains(&child_id) {
    return Ok(());
}
```

### 12.3 Plugin execute re-evaluation 的幂等性

Plugin.execute() 可能被调用多次（如 Start 重复投递）。Re-evaluation 路径是只读检查 + 返回状态，天然幂等。

### 12.4 Stale Failure Check 的幂等性

reconcile_state 在回退计数后将 child 从 `processed_callbacks` 移除。下次检查时该 child 不再出现在 failed_ids 中，不会重复回退。

---

## 13. 与现有机制的兼容策略

### 13.1 渐进式迁移路径

**Phase 1：引入核心机制**

- 实现 `transition_status()` 方法（WorkflowInstance）
- 实现 `should_notify_parent()` 纯函数
- 新增 `WorkflowEvent::ChildRevived` 变体
- 新增 `on_child_revived` → `revive_from_failed` / `revive_from_await`
- SubWorkflowPlugin / ParallelPlugin / ForkJoinPlugin 增加 re-evaluation 逻辑
- Parallel/ForkJoin handle_callback 增加 Stale Failure Check
- 将关键的 `instance.status = X` 改为 `instance.transition_status(X)`
  - 重点位置：`retry_workflow_node`、`skip_workflow_node`

Phase 1 期间，`notify_parent_if_needed` 继续负责 Terminated 事件（不变）。`transition_status` 只新增 Revived 事件。

**Phase 2：统一 Terminated 事件**

- 将 `notify_parent_if_needed` 逻辑迁移到 `transition_status()` 的出站事件中
- 删除 `notify_parent_if_needed` 函数
- 所有终态通知统一由 `transition_status()` 自动产生
- `apply_exec_result` 中的状态变更改用 `transition_status()`

**Phase 3：Task 侧统一（可选）**

- TaskInstance 引入类似的 `transition_status()`
- Task Worker 不再手动投递 NodeCallback
- 由统一转换层自动产生

### 13.2 Phase 1 涉及代码变更

| 文件 | 变更 |
|------|------|
| `domain/workflow/entity/` | 新增 `transition_status()`、`StateTransitionResult`、`validate_workflow_transition` |
| `domain/shared/workflow.rs` | `WorkflowEvent` 新增 `ChildRevived` 变体 |
| `domain/plugin/manager/workflow.rs` | `process_workflow_job` 新增 `ChildRevived` 分支；`on_child_revived` 实现 |
| `domain/plugin/plugins/subworkflow.rs` | `execute()` 增加 re-evaluation 路径（检查已有子实例） |
| `domain/plugin/plugins/parallel.rs` | `execute()` 增加 re-evaluation 路径；`handle_callback` 增加 stale check |
| `domain/plugin/plugins/forkjoin.rs` | 同 parallel |
| `domain/plugin/plugins/http.rs` | `execute()` 增加 re-evaluation 路径（检查已有 TaskInstance） |
| `domain/workflow/service.rs` | `retry_workflow_node`、`skip_workflow_node` 改用 `transition_status()` |
| `domain/workflow/` | 新增 `should_notify_parent` 纯函数 + 单元测试 |

---

## 14. 性能与运维

### 14.1 额外队列消息

| 事件类型 | 触发频率 | 影响 |
|---------|---------|------|
| `Terminated` | 与现有 `NodeCallback` 一致 | 无变化 |
| `Revived` | 仅在用户手动重试时 | 极低频 |
| `Start`（Revived 触发的父重入） | 仅在 Revived 时 | 极低频 |

### 14.2 额外 DB 操作

| 操作 | 场景 | 代价 |
|------|------|------|
| `on_child_revived` | 加载 + CAS 更新父实例 | 1 read + 1 write（与现有 callback 处理相同） |
| Stale Failure Check | 每次 handle_callback | 0-2 次查询（查 failed_ids 对应的实际状态） |
| Plugin re-evaluation | 仅在 Revived 后的 Start 处理中 | 1-N 次查询（查子任务/子工作流实际状态） |

### 14.3 正常路径零影响

当系统运行在"无重试"的正常路径时：
- `should_notify_parent`: Running→Completed 产生 Terminated（与现有 notify_parent 行为一致）
- Stale Failure Check: `failed_ids` 为空 → 短路返回 → 零开销
- Re-evaluation: 不触发（仅在 Revived 后）

---

## 15. 测试策略

### 15.1 单元测试

```rust
#[test]
fn test_should_notify_parent() {
    assert_eq!(
        should_notify_parent(&Running, &Completed),
        Some(ChildEventKind::Terminated(TerminalStatus::Completed))
    );
    assert_eq!(
        should_notify_parent(&Running, &Failed),
        Some(ChildEventKind::Terminated(TerminalStatus::Failed))
    );
    assert_eq!(
        should_notify_parent(&Failed, &Pending),
        Some(ChildEventKind::Revived)
    );
    assert_eq!(should_notify_parent(&Pending, &Running), None);
    assert_eq!(should_notify_parent(&Running, &Await), None);
}

#[test]
fn test_transition_status_with_parent_context() {
    let mut instance = make_child_instance(Some(parent_ctx));
    instance.status = WorkflowInstanceStatus::Failed;
    
    let result = instance.transition_status(WorkflowInstanceStatus::Pending).unwrap();
    assert_eq!(result.outbound_events.len(), 1);
    assert!(matches!(result.outbound_events[0].event, WorkflowEvent::ChildRevived { .. }));
}

#[test]
fn test_transition_status_without_parent_context() {
    let mut instance = make_root_instance(None);
    instance.status = WorkflowInstanceStatus::Failed;
    
    let result = instance.transition_status(WorkflowInstanceStatus::Pending).unwrap();
    assert_eq!(result.outbound_events.len(), 0);
}

#[test]
fn test_stale_failure_check_no_failures() {
    let state = ContainerState { failed_count: 0, results: HashMap::new(), .. };
    let stale = check_stale_failures(&state).await;
    assert!(stale.is_empty());
}
```

### 15.2 集成测试

```rust
#[tokio::test]
async fn test_subworkflow_retry_cascades_to_parent() {
    // Setup: 父 → SubWorkflow → 子已 Failed → 父已 Failed
    let (parent, child) = setup_failed_subworkflow_scenario().await;
    
    // Act: 重试子工作流内部节点
    retry_node_in_child(&child, "failed_http_node").await;
    
    // Assert: 父应恢复为 Pending
    eventually(|| async {
        let p = load_instance(&parent.id).await;
        assert_eq!(p.status, WorkflowInstanceStatus::Pending);
    }).await;
    
    // Act: 模拟 worker 处理 Start + 子完成
    process_all_pending_jobs().await;
    
    // Assert: 父最终成功
    let p = load_instance(&parent.id).await;
    assert_eq!(p.status, WorkflowInstanceStatus::Completed);
}

#[tokio::test]
async fn test_parallel_mixed_children_retry_race_condition() {
    // Setup: Parallel(HTTP*98 + SubWorkflow*2), SubWorkflow-99 失败, 父 Await
    // 99 个已完成, 1 个失败
    let scenario = setup_parallel_mixed_with_one_failure().await;
    
    // Act: 重试子工作流 + 立即完成最后一个 HTTP
    retry_subworkflow_child(&scenario.failed_child).await;
    complete_task(&scenario.last_http_task).await;
    
    // Assert: Stale check 防止误判
    process_all_pending_jobs().await;
    let parent = load_instance(&scenario.parent_id).await;
    // 父不应该是 Failed（因为 stale check 发现子已被重试）
    assert_ne!(parent.status, WorkflowInstanceStatus::Failed);
}
```

---

## 16. 与其他系统的关系

### 16.1 与通知系统（§16）

内部事件通信（本文）与用户通知系统完全独立：

| 维度 | 内部事件通信 | 用户通知系统 |
|------|------------|------------|
| 目的 | 维护父子状态一致性 | 通知人类用户 |
| 接收方 | 父工作流实例 | 人类用户 |
| 事件 | Revived / Terminated | workflow.failed / node.success 等 |

### 16.2 与 Sweeper（§13）

Sweeper 恢复僵尸实例时执行 `Running → Pending`。此转换不在出站规则中（不产生 Revived），正确——Sweeper 恢复的是中间态崩溃，不需要通知父。

### 16.3 与 Skip 节点（§1.4.5）

跳过节点导致 `instance.status: Failed → Pending`。如果是子工作流执行 skip：
- `transition_status(Pending)` → `should_notify_parent(Failed, Pending)` → Revived
- 自动通知父工作流恢复
- 无需 skip 代码路径手动加通知

---

## 17. 总结

### 17.1 核心改动

| 改动 | 说明 |
|------|------|
| `WorkflowInstanceEntity::transition_status()` | 统一状态转换入口 |
| `should_notify_parent()` | 出站规则纯函数 |
| `WorkflowEvent::ChildRevived` | 新事件类型 |
| `on_child_revived` | 父侧处理：`Failed→Pending` + dispatch Start |
| Plugin execute re-evaluation | 所有插件支持"已有子实例时做 gather" |
| Stale Failure Check | 容器 handle_callback 每次校验过期失败 |

### 17.2 解决的问题

| 问题 | 解决方式 |
|------|---------|
| 子工作流重试后父不知道 | `Failed→Pending` 自动产生 Revived |
| 父 Failed 拒绝回调 | 父收到 Revived 后恢复为 Pending→Running→Await，可接收后续回调 |
| 竞争窗口导致误判 | Stale Failure Check 消除时序依赖 |
| 新操作可能遗漏通知 | 通知由 transition_status 自动判定 |
| 混合子任务类型（HTTP+SubWorkflow） | 统一的 check_stale_failures 支持两种实体查询 |

### 17.3 不改变的东西

- Apalis/Redis 队列架构
- epoch/CAS 乐观锁机制
- Worker 模型（Workflow Worker / Task Worker / Sweeper）
- 同步插件（IfCondition / ContextRewrite）的执行方式
- 状态机定义（`Pending` 仍是统一安全边界，不引入新转换）
- MongoDB 数据模型（无新增 collection）

### 17.4 设计原则回顾

1. **通知是状态转换的固有属性**，不是调用者的责任
2. **Pending 是统一安全边界**，所有恢复路径经过 Pending，由 Worker 持锁重入
3. **Plugin 负责 gather**，execute() 具备 re-evaluation 能力，不创建重复子实例
4. **Stale Check 作为防御性校验**，消除事件时序依赖，使系统自修正
5. **零正常路径开销**，所有额外逻辑仅在重试/异常场景触发
