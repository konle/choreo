# 事件驱动状态机架构设计

> 简称 **EDSM** (Event-Driven State Machine)

## 1. 概述与动机

### 1.1 当前架构的痛点

当前工作流引擎采用 **Scatter-Gather + NodeCallback** 模型，事件类型仅两种：

| 事件 | 语义 | 时机 |
|------|------|------|
| `Start` | 冷启动 / 从 Pending 恢复执行 | `execute`、`retry`、`resume` |
| `NodeCallback` | 子节点终态结果 | Task Worker 完成、子工作流完成、用户 skip |

**缺失的语义**：子实体发生**非终态状态变化**时，父工作流无从感知。典型场景：

```
Parallel 容器内某子工作流失败
  → 父工作流 Failed
  → 用户重试子工作流（子 Failed → Pending）
  → 父工作流仍停在 Failed，对子重试一无所知
  → 子完成后 NodeCallback 到达父，父在 Failed 状态下无法处理
```

根因：**事件模型无法表达「子状态变化」这一类重要事实**。

### 1.2 目标

设计一套**彻底抽象、灵活可扩展**的事件驱动状态机架构，满足：

1. **完整的状态变化通知** — 子实体的任何有意义的状态变化都能通知到父
2. **级联天然支持** — 重试、取消、skip 等操作的自然级联，无需硬编码
3. **插件统一接口** — 三类事件走同一入口，插件按需处理
4. **单一 Worker 保证** — CAS + epoch 租约确保一个工作流实例同时只被一个 Worker 处理
5. **可演进** — 新增事件类型不需要改动核心调度循环

### 1.3 核心理念

> **每个工作流实例是一个独立的状态机实体。实体之间只通过事件通信。事件的产生是事实（Fact），状态的变迁是事件投射的投影（Projection）。Worker 是状态机的执行者，CAS+epoch 保证同一实例同一时刻只有一个 Worker 在推进状态机。**

这不是完整的事件溯源（Event Sourcing）——我们不存储事件日志用于回放，状态仍然持久化到 MongoDB。但事件是**驱动状态变迁的唯一手段**，这一点与 Event Sourcing 的精神一致。

---

## 2. 事件模型

### 2.1 事件分类

系统定义三类事件，全部复用 `ExecuteWorkflowJob` 的 Redis/Apalis 队列通道：

```
WorkflowEvent (enum)
  │
  ├── Start                        # 冷启动
  │     实例从 Pending 进入主循环。
  │     触发方：execute、retry、resume 后的 API 投递。
  │
  ├── NodeCallback                 # 终态结果回调
  │     子节点（原子任务或子工作流）到达终态，携带结果。
  │     触发方：Task Worker 完成、子工作流完成、用户 skip。
  │     字段：node_id, child_task_id, status(Success|Failed|Skipped),
  │           output, error_message, input
  │
  └── ChildLifecycle               # 子状态变化通知
        子实体发生非终态状态变化，父需要感知并调整自身。
        触发方：API 层级联投递（retry 子任务/子工作流、cancel 子等）。
        字段：node_id, child_task_id,
              old_status, new_status, meta(Option<Value>)
```

**设计理由**：

- `Start` 和 `NodeCallback` 保留现有语义，零破坏性变更
- `ChildLifecycle` 是新增类型，表达**非终态变化**。与 `NodeCallback` 严格分离，避免同一枚举承担两种语义
- 三类事件走**同一个** `process_workflow_job` 入口，抢同一把 CAS 锁，串行处理

### 2.2 事件产生规则

| 用户操作 | 子实体状态变化 | 对子投递 | 对父投递 |
|----------|---------------|----------|----------|
| execute 工作流 | `Pending → Running` | 无（Worker 内部流转） | 无 |
| retry 工作流 | `Failed → Pending` | `Start` | 无 |
| resume 工作流 | `Suspended → Pending` | `Start` | 无 |
| cancel 工作流 | `Failed/Suspended → Canceled` | 无 | 无 |
| skip 节点 | `Failed → Skipped` | 无 | `NodeCallback { status: Skipped }` |
| retry 子任务（独立） | `Failed → Pending` | `ExecuteTaskJob` | 无 |
| retry 子任务（Parallel 内） | `Failed → Pending` | `ExecuteTaskJob` | `ChildLifecycle { old: Failed, new: Pending }` |
| retry 子工作流 | `Failed → Pending` | `Start` | `ChildLifecycle { old: Failed, new: Pending }` |
| cancel 子工作流 | → `Canceled` | 无 | `NodeCallback { status: Failed, error: "Canceled" }` |
| 子任务执行完成 | → `Success/Failed` | 无 | `NodeCallback { status: Success/Failed, ... }` |
| 子工作流执行完成 | → `Completed/Failed` | 无 | `NodeCallback { status: Success/Failed, ... }` |

**原则**：

- **终态变化** → `NodeCallback`（已有语义，子「完成了」）
- **非终态变化且父需要感知** → `ChildLifecycle`（新增语义，子「变了」）
- **工作流自身的冷启动** → `Start`（不变）

### 2.3 ChildLifecycle 详细字段

```rust
pub struct ChildLifecycleEvent {
    /// 工作流图中节点 ID（Parallel/ForkJoin/SubWorkflow 节点）
    pub node_id: String,
    /// 子任务实例 ID（与 NodeCallback 的 child_task_id 语义对齐）
    pub child_task_id: String,
    /// 变化前状态
    pub old_status: NodeExecutionStatus,
    /// 变化后状态
    pub new_status: NodeExecutionStatus,
    /// 附加元信息（如重试原因、操作人等）
    pub meta: Option<serde_json::Value>,
}
```

`old_status` 和 `new_status` 属于 `NodeExecutionStatus` 枚举，但只使用非终态之间的转换和终态到非终态的转换：

| old_status | new_status | 场景 |
|------------|------------|------|
| `Failed` | `Pending` | 子重试 |
| `Suspended` | `Pending` | 子恢复 |
| `Pending` | `Canceled` | 子取消（可选，也可用 NodeCallback） |
| `Running` | `Pending` | 理论上不出现 |

---

## 3. 工作流状态机

### 3.1 状态机定义

工作流实例状态机与 `docs/architecture.md §1.3` 保持一致，事件驱动下的状态转换表如下：

| 当前状态 | 事件 | 条件/动作 | 目标状态 |
|----------|------|-----------|----------|
| `Pending` | `Start` | 进入主循环，开始执行节点 | `Running` |
| `Running` | 节点需要异步等待 | 分发子任务/子工作流，让出 CPU | `Await` |
| `Running` | 所有节点执行完毕且成功 | — | `Completed` |
| `Running` | 任何节点失败且不可恢复 | — | `Failed` |
| `Running` | 审批/暂停节点 | — | `Suspended` |
| `Running` | 用户取消 | — | `Canceled` |
| `Await` | `NodeCallback`（子完成） | 聚合结果，判断是否继续 | `Pending` (重新进入 Running) 或保持 `Await` |
| `Await` | `ChildLifecycle`（子重试/恢复） | 调整内部计数器，重新进入调度 | `Pending` (→ Running) 或保持 `Await` |
| `Await` | 用户取消 | — | `Canceled` |
| `Suspended` | `Start`（resume） | — | `Pending` |
| `Suspended` | `ChildLifecycle` | 依赖场景（如子工作流恢复） | `Pending` 或保持 `Suspended` |
| `Failed` | `Start`（retry） | 重置失败点，重新执行 | `Pending` |
| `Failed` | `ChildLifecycle`（子重试） | 调整内部状态，从断点恢复 | `Pending` |
| `Canceled` | *任何事件* | 终态，忽略 | `Canceled` |

**关键约束不变**：

1. `Await → Pending`（禁止直达 `Running`），由 Worker 持锁后 `Pending → Running`
2. `Suspended → Pending`（同上）
3. `Failed → Pending`（无论是 retry 还是 ChildLifecycle，都回到安全边界）
4. `Pending` 是统一安全边界——所有非终态到非终态的转换必须经过 `Pending`

### 3.2 事件处理主循环

```
process_workflow_job(job: ExecuteWorkflowJob):
    1. 从 MongoDB 加载 WorkflowInstance（含 epoch）
    2. CAS 抢锁：设置 locked_by, epoch+1
    3. 根据 job.event 路由：
       ├── Start           → run_loop(instance)
       ├── NodeCallback    → handle_callback(instance, callback)
       └── ChildLifecycle  → handle_child_lifecycle(instance, lifecycle_event)
    4. 持久化 WorkflowInstance（CAS 释放锁）
    5. 检查 parent_context：
       → 如有父工作流，投递对应事件给父
```

**所有三类事件在同一入口串行处理**，CAS+epoch 保证同一实例不会并发。

---

## 4. 插件接口演进

### 4.1 新增接口方法

在 `PluginInterface` trait 中新增 `handle_child_lifecycle`：

```rust
#[async_trait]
pub trait PluginInterface: Send + Sync {
    async fn execute(...) -> anyhow::Result<ExecutionResult>;
    async fn handle_callback(...) -> anyhow::Result<ExecutionResult>;

    /// 子实体非终态状态变化通知。
    /// 默认实现：检查实例状态，若为 Failed/Suspended 则转 Pending，否则 NoOp。
    /// 各插件可按需覆写。
    async fn handle_child_lifecycle(
        &self,
        node: &WorkflowNodeInstanceEntity,
        instance: &mut WorkflowInstanceEntity,
        event: &ChildLifecycleEvent,
    ) -> anyhow::Result<LifecycleResult> {
        // 默认：若实例在终态/挂起态，转回 Pending
        if matches!(instance.status, Failed | Suspended) {
            instance.status = WorkflowInstanceStatus::Pending;
            Ok(LifecycleResult::Reschedule)
        } else {
            Ok(LifecycleResult::NoOp)
        }
    }

    fn plugin_type(&self) -> TaskType;
}
```

返回值：

```rust
pub enum LifecycleResult {
    /// 无需重新调度，当前状态不变
    NoOp,
    /// 需要重新进入主循环（类似 Start 后的行为）
    Reschedule,
    /// 需要继续等待（类似 handle_callback 返回 Pending）
    Await,
}
```

### 4.2 各插件的 `handle_child_lifecycle` 实现

| 插件 | 行为 |
|------|------|
| **Http / gRPC / Approval** | 默认 NoOp。这些插件没有子实体需要追踪。 |
| **IfCondition / ContextRewrite** | 默认 NoOp。同步节点，不持有子实体。 |
| **Parallel / ForkJoin** | 核心逻辑：根据 `child_task_id` 定位子任务 → 重置其 `results[child_task_id]` → `failed_count -= 1`（或相应调整）→ 从 `processed_callbacks` 移除 → 重新计算完成条件 → 若需补派任务则标记 → 若实例为 Failed/Suspended 则转 Pending → 返回 `Reschedule` 或 `Await` |
| **SubWorkflow** | 标记子工作流正在重试 → 若实例为 Failed/Suspended 则转 Pending → 返回 `Reschedule` |
| **Llm** | 同 Http（默认 NoOp） |

### 4.3 Parallel/ForkJoin 的续跑机制

当父工作流通过 `Start` 或 `ChildLifecycle` 重入 Parallel/ForkJoin 节点时，`execute` 需要支持「续跑」而非「重新播种」：

```
ParallelPlugin::execute(node, instance):
    output = node.task_instance.output

    if output 已存在且 dispatched_count > 0:
        // ── 续跑模式 ──
        // 检查是否有需要重新派发的子任务
        //   - results[child_id] == null 或 status == Failed 的子任务
        //   - 但不在 processed_callbacks 中（表示尚未被 handle_callback 收走）
        missing = compute_missing_tasks(output)
        if missing.is_empty():
            // 所有子任务已在途，无需补派
            return ExecutionResult::Pending  // 继续等
        else:
            // 补派缺失的子任务
            dispatch_tasks(missing)
            return ExecutionResult::Pending
    else:
        // ── 首次播种模式（现有逻辑）──
        initialize_state_machine(output)
        dispatch_initial_batch()
        return ExecutionResult::Pending
```

**关键**：续跑模式**不清零 `success_count`、`dispatched_count`** 等已有状态，只补派缺失的子任务。这保证了已完成子任务的结果不丢失。

---

## 5. 级联机制

### 5.1 原则

级联（Cascade）是指用户对子实体执行操作时，需要同步修正父链状态并投递事件以唤醒父实体的过程。

**核心原则**：

1. **级联是 API 层的显式编排，不是引擎内部的隐式递归**
2. **级联遵循两阶段**：阶段 1 持久化，阶段 2 投递
3. **级联深度**：理论上支持无限嵌套，每层父级都需要修正状态和投递事件

### 5.2 级联投递规则表

#### 5.2.1 重试子任务（Parallel/ForkJoin 内）

```
用户操作：POST /api/v1/task/instances/{child_task_id}/retry

API 层面：
  阶段 1 持久化：
    - 查找 child_task 所属的 parent_workflow_instance 和 parent_node_id
    - child_task.task_status = Pending
    - （可选）清除 child_task 的 output/error_message
    
  阶段 2 投递：
    - dispatch_task(ExecuteTaskJob { child_task_id })         // 子任务重新执行
    - dispatch_workflow(ExecuteWorkflowJob {
        instance_id: parent_instance_id,
        event: ChildLifecycle {
          node_id: parent_node_id,
          child_task_id: child_task_id,
          old_status: Failed,
          new_status: Pending,
        }
      })                                                        // 通知父工作流
```

#### 5.2.2 重试子工作流

```
用户操作：POST /api/v1/workflow/instances/{child_instance_id}/retry

API 层面：
  阶段 1 持久化：
    - child_instance.status = Pending
    - parent_node.status = Pending  （SubWorkflow 节点恢复为可执行）
    - parent_instance.status = Pending  （父工作流恢复）
    
  阶段 2 投递：
    - dispatch_workflow(ExecuteWorkflowJob {
        instance_id: child_instance_id,
        event: Start,
      })                                                        // 子工作流重新执行
    - dispatch_workflow(ExecuteWorkflowJob {
        instance_id: parent_instance_id,
        event: ChildLifecycle {
          node_id: parent_node_id,
          child_task_id: parent_task_instance_id,
          old_status: Failed,
          new_status: Pending,
        }
      })                                                        // 通知父工作流
```

#### 5.2.3 嵌套级联

如果父工作流本身也是子工作流（深度 > 0），级联继续向上：

```
API 层面：
  阶段 1 持久化（自底向上依次修正）：
    - child_instance: Failed → Pending
    - parent_node: Failed → Pending
    - parent_instance: Failed → Pending
    - grandparent_node: （如果处于 Failed/Suspended，修正为 Pending）
    - grandparent_instance: Failed → Pending
    
  阶段 2 投递（自底向上依次投递）：
    - dispatch_workflow(child_instance, Start)
    - dispatch_workflow(parent_instance, ChildLifecycle { ... })
    - dispatch_workflow(grandparent_instance, ChildLifecycle { ... })
```

**注意**：阶段 1 的持久化需要在一个**多文档事务**（MongoDB 4.0+ session transaction）中完成，确保所有层的状态变更原子性。若事务不可用，则采用逆序写入：先写最外层，最后写最内层，通过 CAS epoch 保证一致性。

### 5.3 级联中止条件

级联向上传播时，遇到以下条件之一停止：

1. **父工作流处于非终态**（Running / Await）—— 父已经在处理中，不需要被唤醒
2. **父工作流处于终态 Canceled** —— 终态不可逆
3. **到达根工作流**（parent_context == null）—— 无更上层
4. **父工作流 Failed/Suspended 但其对应节点不是 SubWorkflow 类型** —— 说明父不是因为子工作流失败的，级联无效

### 5.4 级联与现有 skip 的统一

现有的 skip 节点操作（`NodeCallback { status: Skipped }`）不变。这是终态通知，语义明确。

在 EDSM 模型下，所有用户操作按是否涉及级联分类：

| 用户操作 | 涉及级联 | 对父投递 |
|----------|---------|----------|
| skip 节点 | 否（已是终态） | `NodeCallback { Skipped }` |
| retry 子任务（嵌套） | 是 | `ChildLifecycle { Failed→Pending }` |
| retry 子工作流 | 是 | `ChildLifecycle { Failed→Pending }` |
| cancel 实例 | 否（终态） | 无（或 `NodeCallback { Canceled }` 通知父） |
| cancel 子工作流 | 是 | `NodeCallback { Failed, error: "Canceled" }` |

---

## 6. 场景详演

### 6.1 场景一：Parallel 内子任务重试

**初始状态**：父工作流有 Parallel 节点 node_6，包含 100 个子任务。其中第 3 个子任务失败，触发 `max_failures=2` 导致整个 Parallel 失败，父工作流 `Failed`。

```
1. 用户：POST /api/v1/task/instances/xxx-node_6-2/retry
   
2. API 阶段 1 持久化：
   - task_instance[xxx-node_6-2].task_status = Pending
   
3. API 阶段 2 投递：
   - dispatch_task(ExecuteTaskJob { task_instance_id: "xxx-node_6-2" })
   - dispatch_workflow(ExecuteWorkflowJob {
       instance_id: parent_workflow_id,
       event: ChildLifecycle {
         node_id: "node_6",
         child_task_id: "xxx-node_6-2",
         old_status: Failed,
         new_status: Pending,
       }
     })

4. 父 Worker 消费 ChildLifecycle 事件：
   a. CAS 抢锁
   b. 路由到 ParallelPlugin::handle_child_lifecycle()
   c. ParallelPlugin 处理：
      - 从 results["xxx-node_6-2"] 读取原状态 Failed
      - failed_count -= 1（从 2 变为 1，熔断解除）
      - 从 processed_callbacks 移除 "xxx-node_6-2"
      - 清除 results["xxx-node_6-2"]（设为 null）
      - 实例 status: Failed → Pending
   d. 持久化并释放锁

5. 父 Worker 消费 Start 事件（因为实例回到了 Pending）：
   a. CAS 抢锁
   b. 进入 run_loop
   c. current_node = "node_6"
   d. ParallelPlugin::execute() 检查 output：
      - dispatched_count = 100, 已经派发过
      - success_count = 97, failed_count = 1 (之前还有一个失败)
      - missing = [xxx-node_6-2]（results 为 null 且不在 processed_callbacks 中）
      - 补派第 3 个子任务
   e. 实例 Running → Await
   
6. 第 3 个子任务完成：
   → NodeCallback { node_id: "node_6", child_task_id: "xxx-node_6-2", status: Success }
   → 父 Worker 醒来，handle_callback 聚合
   → success_count = 98, failed_count = 1
   → 未完成全员收工，继续等待
   
7. 之前失败的第 1 个子任务也被其他手段修复...
   → 最终 success_count + failed_count == 100
   → Parallel 完成，父工作流继续下一节点
```

### 6.2 场景二：嵌套子工作流重试

**初始状态**：父工作流 A 有 SubWorkflow 节点 node_3，指向子工作流 B。B 内部失败。A 和 B 都处于 Failed。

```
1. 用户：POST /api/v1/workflow/instances/{B_instance_id}/retry

2. API 阶段 1 持久化（级联向上）：
   - B_instance.status = Pending
   - A_node_3.status = Pending  （SubWorkflow 节点）
   - A_instance.status = Pending

3. API 阶段 2 投递：
   - dispatch_workflow(B_instance_id, Start)
   - dispatch_workflow(A_instance_id, ChildLifecycle {
       node_id: "node_3",
       child_task_id: A_node_3.task_instance_id,
       old_status: Failed,
       new_status: Pending,
     })

4. 子工作流 B 重新执行（Start）：
   → 正常跑完 → Completed
   → B Worker 检测 parent_context 非空
   → 投递 NodeCallback 给 A

5. 父工作流 A 消费 ChildLifecycle：
   → A 的 SubWorkflowPlugin::handle_child_lifecycle()
   → 将 node_3 标记为「子工作流正在重试」
   → A 从 Pending → Running（run_loop）
   → 遇到 node_3（SubWorkflow），发现子工作流状态为 Pending/Running
   → 将 node_3 置 Suspended，A → Await

注：步骤 4 和 5 时序可能交叉。如果 B 完成得比 A 处理 ChildLifecycle 快，
   则 NodeCallback 会排队等待。A 先处理 ChildLifecycle 进入 Await，
   然后处理 NodeCallback 正常推进。两者抢同一把 CAS 锁，串行处理，无冲突。
```

### 6.3 场景三：三层嵌套级联

**初始状态**：A（根工作流）→ B（子工作流）→ C（孙工作流）。C 内部某节点失败。A、B、C 都 Failed。

```
1. 用户：POST /api/v1/workflow/instances/{C_instance_id}/retry

2. API 阶段 1 持久化（逆序写入，保证一致性）：
   - A_instance.status = Pending, epoch += 1
   - A_node_B.status = Pending
   - B_instance.status = Pending, epoch += 1
   - B_node_C.status = Pending
   - C_instance.status = Pending, epoch += 1

3. API 阶段 2 投递（自底向上）：
   - dispatch_workflow(C_instance_id, Start)
   - dispatch_workflow(B_instance_id, ChildLifecycle { node_id: B_node_C, ... })
   - dispatch_workflow(A_instance_id, ChildLifecycle { node_id: A_node_B, ... })

4. 三个事件依次被 Worker 消费：
   - C: Start → C 重新执行
   - B: ChildLifecycle → B 调整状态，等待 C 完成
   - A: ChildLifecycle → A 调整状态，等待 B 完成
   
5. 事件驱动链式完成：
   C 完成 → NodeCallback 给 B → B 完成 → NodeCallback 给 A → A 继续
```

### 6.4 场景四：Parallel 内子任务 skip（已有实现，确认兼容）

```
用户：POST /api/v1/workflow/instances/{id}/skip-node
Body: { "node_id": "node_6", "child_task_id": "xxx-node_6-3", "output": {} }

API 阶段 1：
  - node_6.task_instance.output.results["xxx-node_6-3"] = { status: Skipped, output: {} }
  - task_instances["xxx-node_6-3"].task_status = Completed
  - parent_instance.status = Pending

API 阶段 2：
  - dispatch_workflow(NodeCallback {
      node_id: "node_6",
      child_task_id: "xxx-node_6-3",
      status: Skipped,
      output: {},
    })

父 Worker 处理：
  → ParallelPlugin::handle_callback()
  → success_count += 1, skipped_count += 1（与现有逻辑一致）
  → 与正常 ChildLifecycle 无冲突
```

**兼容性**：skip 走 `NodeCallback` 通道是正确的——它表达的是「子节点已到达终态 Skipped」，不是中间态变化。

---

## 7. Cancel 与 Await 下取消的补充

### 7.1 当前实现的问题

§1.3 状态机中 `Await → Canceled` 已定义，但**当前实现未支持**。用户无法取消正在等待回调的工作流实例。

### 7.2 EDSM 下的 Cancel 语义

Cancel 操作的持久化与投递：

```
用户：POST /api/v1/workflow/instances/{id}/cancel

API 阶段 1 持久化：
  - instance.status = Canceled
  - 所有 Pending/Running 的节点 status = Canceled
  
API 阶段 2 投递：
  - 无需投递编排 Job（终态不可逆，Worker 不会再处理）
  
幂等保护：
  - 若Cancel后仍有滞留的NodeCallback/ChildLifecycle到达，
    Worker 检测到instance.status == Canceled，
    直接丢弃事件（ACK但不处理）
```

### 7.3 Await 下取消

```
用户：POST /api/v1/workflow/instances/{id}/cancel（实例当前状态为 Await）

API 阶段 1 持久化：
  - instance.status: Await → Canceled
  
API 阶段 2 投递：
  - 无需投递编排 Job
  - 后续到达的 NodeCallback 被幂等丢弃
  
级联（如需通知父）：
  - 若被取消的是子工作流：
    dispatch_workflow(parent_instance_id, NodeCallback {
      node_id: parent_node_id,
      child_task_id: parent_task_instance_id,
      status: Failed,
      error_message: "Child workflow canceled",
    })
```

---

## 8. 与现有实现的兼容与迁移

### 8.1 不破坏现有的接口和行为

| 现有行为 | EDSM 下 | 兼容性 |
|----------|---------|--------|
| `Start` 事件 | 不变 | ✅ 完全兼容 |
| `NodeCallback` 事件 | 不变 | ✅ 完全兼容 |
| skip 走 `NodeCallback` | 不变 | ✅ 完全兼容 |
| retry 工作流 `Start` | 不变 | ✅ 完全兼容 |
| Parallel `handle_callback` | 不变 | ✅ 完全兼容 |
| CAS + epoch 锁机制 | 不变 | ✅ 完全兼容 |

### 8.2 新增内容

| 新增项 | 说明 |
|--------|------|
| `WorkflowEvent::ChildLifecycle` 变体 | 枚举新增，序列化/反序列化兼容 |
| `PluginInterface::handle_child_lifecycle` 方法 | trait 新增，有默认实现（NoOp 或自动回 Pending） |
| `ParallelPlugin::handle_child_lifecycle` | 核心逻辑：调整计数器、重新派发 |
| `SubWorkflowPlugin::handle_child_lifecycle` | 标记子工作流重试中 |
| `ParallelPlugin::execute` 续跑检测 | 检查已有 output，补派缺失子任务 |
| API 层级联投递逻辑 | retry 子任务/子工作流时，向上投递 `ChildLifecycle` |
| MongoDB 多文档事务（级联） | 多层嵌套时的原子性保证（可选，可用逆序写入替代） |

### 8.3 迁移步骤

| 阶段 | 内容 | 依赖 |
|------|------|------|
| **P0** | `WorkflowEvent::ChildLifecycle` 枚举定义 + Worker 入口路由 | 无 |
| **P0** | `PluginInterface::handle_child_lifecycle` 默认实现 | P0 |
| **P0** | `ParallelPlugin::handle_child_lifecycle` 实现 | P0 |
| **P0** | `ParallelPlugin::execute` 续跑检测 | P0 |
| **P1** | retry 子任务 API 级联投递 `ChildLifecycle` | P0 |
| **P1** | retry 子工作流 API 级联投递 `ChildLifecycle` + `Start` | P0 |
| **P1** | `SubWorkflowPlugin::handle_child_lifecycle` 实现 | P0 |
| **P2** | 多层嵌套级联（递归向上） | P1 |
| **P2** | Cancel 实例（含 Await 下取消） | P0 |
| **P2** | `Await` 下取消级联通知 | P2 |

---

## 9. 未来扩展

EDSM 架构天然支持以下扩展（仅需新增事件类型，无需改动核心循环）：

### 9.1 定时器事件

```rust
WorkflowEvent::TimerFired {
    timer_id: String,
    node_id: String,
    payload: Option<Value>,
}
```

用于实现 Pause 节点的自动超时唤醒、Approval 的超时自动驳回等。

### 9.2 外部信号事件

```rust
WorkflowEvent::ExternalSignal {
    signal_type: String,  // "webhook", "api_call", "event_bridge" 等
    node_id: Option<String>,
    payload: Value,
}
```

用于实现「等待外部 Webhook 回调」节点类型。工作流进入 `Await`，外部信号到达时通过 `ExternalSignal` 唤醒。

### 9.3 审批/人工干预事件

```rust
WorkflowEvent::ApprovalDecided {
    approval_id: String,
    node_id: String,
    decision: Decision,  // Approve / Reject
    comment: Option<String>,
}
```

与当前 Approval 流程合并，审批结果从 API 直接调引擎改为走事件队列。

### 9.4 条件订阅/事件路由

未来可引入**事件总线**（如 Redis Pub/Sub + 事件过滤器），让工作流实例订阅感兴趣的外部事件类型，而不仅仅是自己子节点的事件。这为以下场景铺路：

- 工作流 A 等待工作流 B 的状态变化（非父子关系）
- 工作流等待外部系统的信号
- 跨租户的事件通知

---

## 10. 总结

### 10.1 设计原则总结

| 原则 | 实现 |
|------|------|
| **事件是唯一的通信手段** | 三类事件覆盖所有状态变化场景 |
| **单一 Worker 保证** | CAS + epoch 租约，不变 |
| **状态机是投影** | 实例状态是事件序列的聚合结果，直接持久化 |
| **级联是显式编排** | API 层负责两阶段：持久化 + 投递 |
| **插件统一接口** | `execute` / `handle_callback` / `handle_child_lifecycle` |
| **续跑而非重跑** | Parallel/ForkJoin 检测已有状态机，只补发缺失子任务 |
| **终态与中间态分离** | `NodeCallback` 承载终态，`ChildLifecycle` 承载中间态 |

### 10.2 与原架构文档的关系

本方案**不替换** `docs/architecture.md` 中已有的设计，而是在其基础上**演进**：

- §1.3 状态机：**不变**，EDSM 是状态机的驱动方式
- §1.4 重试/级联：从 P2 规划升级为正式实现，采用 EDSM 模型
- §2 插件系统：**扩展** `PluginInterface` trait
- §3 Parallel：**扩展** `execute`(续跑) + `handle_child_lifecycle`
- §6 SubWorkflow：**扩展** `handle_child_lifecycle`
- §4 CAS/epoch：**不变**

### 10.3 一句话总结

> **把「子节点状态变化」从隐式假设变为显式事件，让父工作流能对子的每一次有意义的状态跃迁做出响应——这就是 EDSM 的本质。**