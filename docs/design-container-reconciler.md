# ContainerReconciler 设计文档：容器状态推导模型

## 1. 背景与动机

### 1.1 当前机制的局限

容器插件（Parallel/ForkJoin）在 `handle_callback` 中通过维护**计数器**（`success_count`、`failed_count`）来追踪子任务完成度。计数器作为 "stored state"，在以下场景可能与 DB 中子任务的真实状态产生偏差：

| 场景 | 计数器状态 | 真实状态 | 后果 |
|------|-----------|---------|------|
| 子任务被重试（`Failed → Pending`）| `failed_count` 仍含此子任务 | 子任务实际是 Pending/Running | 可能误触发 `early_abort` |
| Stale Check 回退后 callback 被当重复丢弃 | `processed_callbacks` 已移除该子任务 | 子任务已完成但无新 callback | 父永远卡在 Await |
| 外部操作修改子任务状态（Skip/Cancel）| 计数器未同步 | 状态已变 | 决策错误 |

### 1.2 Phase 1 已有的 Stale Failure Check

Phase 1 引入了 Stale Failure Check：每次 `handle_callback` 时检查之前标记为 "Failed" 的子任务是否仍然是 Failed。这是一个**部分推导**——只检查 failed 的子集。

### 1.3 Full Reconcile 的定位

ContainerReconciler 提供 **Full Reconcile** 能力：在容器做终态决策（`all_done` 或 `early_abort`）之前，一次性查询所有子任务的 DB 真实状态，验证计数器是否正确。

**它不是替代 Stale Check，而是在决策点提供额外的正确性保证。**

---

## 2. 设计目标

| 目标 | 说明 |
|------|------|
| **正确性** | 容器的终态决策（Success/Failed）基于子任务的 DB 真实状态，不依赖可能漂移的计数器 |
| **零正常路径开销** | 只在 `apparently_all_done` 或 `apparently_early_abort` 时触发（每个容器生命周期 1-2 次）|
| **不改变状态机** | 不引入新的状态转换，不改变 CAS/epoch 机制 |
| **不污染接口** | PluginInterface 和 PluginExecutor trait 不变 |
| **显式依赖** | 只有容器插件持有 Reconciler 引用 |

---

## 3. 核心数据结构

### 3.1 ReconcileResult

```rust
/// Full Reconcile 的结果：从 DB 推导的子任务聚合状态
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    /// 已完成（Completed）的子任务数
    pub actual_completed: u64,
    /// 已失败（Failed）的子任务数
    pub actual_failed: u64,
    /// 仍在运行中（Pending + Running）的子任务数
    pub actual_running: u64,
    /// 已跳过（Skipped）的子任务数
    pub actual_skipped: u64,
    /// 已取消（Canceled）的子任务数
    pub actual_canceled: u64,
    /// 容器 state 中标记为 Failed 但 DB 中已不是 Failed 的子任务 ID
    pub stale_failures: Vec<String>,
    /// 总查询数（应等于 dispatched_count）
    pub total_queried: u64,
}

impl ReconcileResult {
    /// 是否真的全部完成（没有还在跑的子任务）
    pub fn is_truly_all_done(&self) -> bool {
        self.actual_running == 0
    }

    /// 是否有真实的失败
    pub fn has_real_failures(&self) -> bool {
        self.actual_failed > 0
    }

    /// 成功 + 跳过 + 取消 = 非失败终态
    pub fn non_failure_terminal_count(&self) -> u64 {
        self.actual_completed + self.actual_skipped + self.actual_canceled
    }
}
```

### 3.2 ChildStatus（内部辅助）

```rust
/// 子任务状态查询结果
#[derive(Debug, Clone)]
struct ChildStatus {
    pub child_id: String,
    pub status: TaskInstanceStatus,
}
```

---

## 4. ContainerReconciler 实现

### 4.1 结构定义

```rust
use std::sync::Arc;
use crate::shared::workflow::TaskInstanceStatus;
use crate::task::service::TaskInstanceService;

/// 容器状态协调器
///
/// 职责：从 DB 推导容器子任务的真实聚合状态
/// 不负责：修改容器 state、做业务决策、dispatch 事件
pub struct ContainerReconciler {
    task_instance_svc: Arc<TaskInstanceService>,
}

impl ContainerReconciler {
    pub fn new(task_instance_svc: Arc<TaskInstanceService>) -> Self {
        Self { task_instance_svc }
    }

    /// 批量查询所有子任务的真实状态，返回聚合结果
    ///
    /// # Arguments
    /// - `child_task_ids`: 所有已 dispatch 的子任务 ID 列表
    ///
    /// # Performance
    /// - 底层使用 `$in` 查询，一次 DB round-trip
    /// - 只在容器终态决策点调用（每容器生命周期 1-2 次）
    pub async fn reconcile_task_children(
        &self,
        child_task_ids: &[String],
    ) -> anyhow::Result<ReconcileResult> {
        if child_task_ids.is_empty() {
            return Ok(ReconcileResult {
                actual_completed: 0,
                actual_failed: 0,
                actual_running: 0,
                actual_skipped: 0,
                actual_canceled: 0,
                stale_failures: vec![],
                total_queried: 0,
            });
        }

        let statuses = self
            .task_instance_svc
            .batch_get_statuses(child_task_ids)
            .await?;

        let mut actual_completed = 0u64;
        let mut actual_failed = 0u64;
        let mut actual_running = 0u64;
        let mut actual_skipped = 0u64;
        let mut actual_canceled = 0u64;

        for (_id, status) in &statuses {
            match status {
                TaskInstanceStatus::Completed => actual_completed += 1,
                TaskInstanceStatus::Failed => actual_failed += 1,
                TaskInstanceStatus::Pending | TaskInstanceStatus::Running => actual_running += 1,
                TaskInstanceStatus::Skipped => actual_skipped += 1,
                TaskInstanceStatus::Canceled => actual_canceled += 1,
            }
        }

        Ok(ReconcileResult {
            actual_completed,
            actual_failed,
            actual_running,
            actual_skipped,
            actual_canceled,
            stale_failures: vec![], // 由调用者比对 state 填充
            total_queried: statuses.len() as u64,
        })
    }
}
```

### 4.2 TaskInstanceService 新增方法

```rust
impl TaskInstanceService {
    /// 批量查询任务实例状态
    /// 底层使用 MongoDB `$in` 查询，单次 round-trip
    pub async fn batch_get_statuses(
        &self,
        task_instance_ids: &[String],
    ) -> anyhow::Result<Vec<(String, TaskInstanceStatus)>> {
        self.repository
            .batch_get_statuses(task_instance_ids)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }
}
```

### 4.3 Repository 层

```rust
// trait TaskInstanceRepository
async fn batch_get_statuses(
    &self,
    task_instance_ids: &[String],
) -> Result<Vec<(String, TaskInstanceStatus)>, RepositoryError>;

// MongoDB 实现
async fn batch_get_statuses(
    &self,
    task_instance_ids: &[String],
) -> Result<Vec<(String, TaskInstanceStatus)>, RepositoryError> {
    let filter = doc! {
        "task_instance_id": { "$in": task_instance_ids }
    };
    let projection = doc! {
        "task_instance_id": 1,
        "task_status": 1,
    };
    // 使用 projection 只查 status 字段，减少网络传输
    let cursor = self.collection
        .find(filter)
        .projection(projection)
        .await?;
    
    let results: Vec<(String, TaskInstanceStatus)> = cursor
        .map(|doc| (doc.task_instance_id, doc.task_status))
        .collect()
        .await;
    
    Ok(results)
}
```

---

## 5. 插件注入模式

### 5.1 ParallelPlugin 改造

```rust
pub struct ParallelPlugin {
    reconciler: Option<Arc<ContainerReconciler>>,
}

impl ParallelPlugin {
    pub fn new() -> Self {
        Self { reconciler: None }
    }

    pub fn with_reconciler(mut self, reconciler: Arc<ContainerReconciler>) -> Self {
        self.reconciler = Some(reconciler);
        self
    }
}
```

### 5.2 ForkJoinPlugin 同理

```rust
pub struct ForkJoinPlugin {
    reconciler: Option<Arc<ContainerReconciler>>,
}

impl ForkJoinPlugin {
    pub fn new() -> Self {
        Self { reconciler: None }
    }

    pub fn with_reconciler(mut self, reconciler: Arc<ContainerReconciler>) -> Self {
        self.reconciler = Some(reconciler);
        self
    }
}
```

### 5.3 引擎注册时注入

```rust
// engine.rs - create_plugin_manager
let reconciler = Arc::new(ContainerReconciler::new(task_instance_svc.clone()));

manager.register(Box::new(
    ParallelPlugin::new().with_reconciler(reconciler.clone()),
));
manager.register(Box::new(
    ForkJoinPlugin::new().with_reconciler(reconciler.clone()),
));
```

---

## 6. 容器插件中的使用方式

### 6.1 Parallel handle_callback 中的调用

```rust
// 现有的计数器更新逻辑不变...
let apparently_all_done = success_count + failed_count == total_items;
let apparently_abort = match max_failures {
    Some(max) => failed_count >= max as u64,
    None => false,
};

// === Full Reconcile 决策点 ===
if (apparently_all_done || apparently_abort) && self.reconciler.is_some() {
    let reconciler = self.reconciler.as_ref().unwrap();
    
    // 构建所有已 dispatch 的子任务 ID
    let child_ids: Vec<String> = (0..dispatched_count as usize)
        .map(|i| format!("{}-{}-{}", 
            workflow_instance.workflow_instance_id, 
            node_instance.node_id, 
            i
        ))
        .collect();
    
    let reconciled = reconciler.reconcile_task_children(&child_ids).await?;
    
    if !reconciled.is_truly_all_done() {
        // 还有子任务在跑（被重试了）→ 不做终态决策
        debug!(
            node_id = %node_instance.node_id,
            actual_running = reconciled.actual_running,
            "parallel: reconcile prevented premature termination, continuing Await"
        );
        
        // 修正计数器（可选：让 state 和真实状态同步）
        state["success_count"] = serde_json::json!(reconciled.actual_completed);
        state["failed_count"] = serde_json::json!(reconciled.actual_failed);
        node_instance.task_instance.output = Some(state);
        
        return Ok(ExecutionResult {
            status: NodeExecutionStatus::Await,
            dispatch_jobs: vec![],
            dispatch_workflow_jobs: vec![],
            jump_to_node: None,
        });
    }
    
    // 真的全完成了，用真实数据做最终决策
    let real_all_done = reconciled.is_truly_all_done();
    let real_has_failures = reconciled.has_real_failures();
    let real_abort = match max_failures {
        Some(max) => reconciled.actual_failed >= max as u64,
        None => false,
    };
    
    // 修正计数器
    success_count = reconciled.actual_completed + reconciled.actual_skipped;
    failed_count = reconciled.actual_failed;
    
    // 继续到下面的正常决策逻辑（基于修正后的计数器）
}

// 原有的决策逻辑（使用可能被 reconcile 修正过的计数器）
let exec_result = if apparently_abort { ... } else if all_done && failed_count > 0 { ... } ...
```

### 6.2 Reconcile 不可用时的降级

如果 `reconciler` 为 `None`（如单元测试中未注入），则退化为现有行为——仅依赖计数器 + Stale Check。

---

## 7. 与现有 Stale Failure Check 的关系

### 7.1 共存策略（推荐）

| 机制 | 触发时机 | 作用 | 开销 |
|------|---------|------|------|
| **Stale Check** | 每次 handle_callback | 逐步修正被重试的 failed 子任务计数 | O(failed_count) 次单个查询 |
| **Full Reconcile** | 终态决策点（all_done/abort） | 全量验证，防止任何遗漏 | 1 次批量查询（O(1) round-trip）|

**为什么共存而非替代：**
1. Stale Check 在**每次 callback** 时执行，逐步修正计数器 → 减少累积偏差
2. Full Reconcile 在**决策点**执行，作为最终防线 → 防止任何遗漏
3. 如果移除 Stale Check，计数器的漂移会更大（虽然 reconcile 能修正，但日志/debug 中看到的中间状态会很混乱）

### 7.2 未来可选：替代策略

如果验证 reconcile 稳定可靠，可以：
1. 移除 Stale Check
2. 移除 PluginExecutor 上的 `is_task_still_failed` 方法
3. 计数器变为纯"快照"（用于快速路径判断 + 日志）
4. 所有终态决策完全由 reconcile 驱动

---

## 8. 性能分析

### 8.1 正常路径（无重试）

```
子任务-0 完成 → callback → 计数器+1 → all_done? 1≠N → Await（不触发 reconcile）
子任务-1 完成 → callback → 计数器+1 → all_done? 2≠N → Await（不触发 reconcile）
...
子任务-N 完成 → callback → 计数器+1 → all_done? N==N → 触发 reconcile!
  → 1 次批量查询（`$in` 查询 N 个 ID）
  → reconcile 确认全部完成 → Success
```

**正常路径额外开销：1 次批量 DB 查询（仅在最后一个 callback 时）。**

### 8.2 异常路径（有重试）

```
子任务-99（最后一个）完成 → all_done? success+failed==N → 触发 reconcile
  → 发现有子任务被重试了（actual_running > 0）→ 阻止终态 → Await
  → 被重试的子任务完成后 callback → 再次 all_done → 再次 reconcile → 确认完成
```

**异常路径：最多 2 次 reconcile（一次被拦截，一次确认完成）。**

### 8.3 MongoDB `$in` 查询性能

```javascript
db.task_instances.find(
    { task_instance_id: { $in: ["id-0", "id-1", ..., "id-99"] } },
    { task_instance_id: 1, task_status: 1 }
)
```

- `task_instance_id` 有唯一索引 → `IXSCAN`
- Projection 只查 2 个字段 → 网络传输极小
- 100 个 ID 的 `$in` 查询：< 5ms（本地 MongoDB）
- 1000 个 ID 的 `$in` 查询：< 20ms

---

## 9. 和 EBUSY 方案的关系

ContainerReconciler 和 EBUSY 解决的是**不同层面**的问题：

| 层面 | 问题 | 解决方案 |
|------|------|---------|
| **API 准入控制** | 用户连续重试多个子任务时被 409 拒绝 | EBUSY (409) |
| **容器决策正确性** | 计数器可能因重试/Skip/异常而偏差 | ContainerReconciler |

两者是互补的，不是替代关系。

---

## 10. 对 Worker 独占性和 CAS 的影响

### 10.1 零影响

Reconcile 在以下条件下执行：
1. Worker 已持有 lock（`handle_callback` 在 `process_workflow_job` 中执行，已 `acquire_lock`）
2. 是**只读**操作（查询 `task_instances` 集合，不写入）
3. 在 `apply_exec_result`（CAS 写入）**之前**执行
4. 读取的是**其他集合**（task_instances），不是当前实例本身

CAS 保证：依然是单次 `save_instance_and_bump_epoch` 原子写入。Reconcile 只影响 `handle_callback` 的返回值（`ExecutionResult`），不引入额外写入。

### 10.2 与 Task Worker 的并发

Reconcile 读取子 TaskInstance 时，Task Worker 可能正在修改同一个 TaskInstance（如 Running → Completed）。

**最坏情况**：读到 Running → 认为还在跑 → 返回 Await
**后果**：子任务完成后会再发一个 callback → 下次 reconcile 读到正确状态
**结论**：最终一致，无正确性问题

---

## 11. 测试策略

### 11.1 ContainerReconciler 单元测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Mock TaskInstanceService
    struct MockTaskSvc {
        statuses: HashMap<String, TaskInstanceStatus>,
    }

    #[tokio::test]
    async fn test_reconcile_all_completed() {
        let svc = mock_svc(vec![
            ("child-0", TaskInstanceStatus::Completed),
            ("child-1", TaskInstanceStatus::Completed),
        ]);
        let reconciler = ContainerReconciler::new(Arc::new(svc));
        
        let result = reconciler.reconcile_task_children(&["child-0", "child-1"]).await.unwrap();
        
        assert_eq!(result.actual_completed, 2);
        assert_eq!(result.actual_running, 0);
        assert!(result.is_truly_all_done());
        assert!(!result.has_real_failures());
    }

    #[tokio::test]
    async fn test_reconcile_with_retried_child() {
        let svc = mock_svc(vec![
            ("child-0", TaskInstanceStatus::Completed),
            ("child-1", TaskInstanceStatus::Running), // 被重试了
        ]);
        let reconciler = ContainerReconciler::new(Arc::new(svc));
        
        let result = reconciler.reconcile_task_children(&["child-0", "child-1"]).await.unwrap();
        
        assert_eq!(result.actual_completed, 1);
        assert_eq!(result.actual_running, 1);
        assert!(!result.is_truly_all_done()); // 不能判定为全完成
    }

    #[tokio::test]
    async fn test_reconcile_mixed_statuses() {
        let svc = mock_svc(vec![
            ("child-0", TaskInstanceStatus::Completed),
            ("child-1", TaskInstanceStatus::Failed),
            ("child-2", TaskInstanceStatus::Skipped),
            ("child-3", TaskInstanceStatus::Pending),
        ]);
        let reconciler = ContainerReconciler::new(Arc::new(svc));
        
        let result = reconciler.reconcile_task_children(
            &["child-0", "child-1", "child-2", "child-3"]
        ).await.unwrap();
        
        assert_eq!(result.actual_completed, 1);
        assert_eq!(result.actual_failed, 1);
        assert_eq!(result.actual_skipped, 1);
        assert_eq!(result.actual_running, 1);
        assert!(!result.is_truly_all_done());
        assert!(result.has_real_failures());
    }

    #[tokio::test]
    async fn test_reconcile_empty() {
        let svc = mock_svc(vec![]);
        let reconciler = ContainerReconciler::new(Arc::new(svc));
        
        let result = reconciler.reconcile_task_children(&[]).await.unwrap();
        assert_eq!(result.total_queried, 0);
        assert!(result.is_truly_all_done());
    }
}
```

### 11.2 Parallel 集成测试

```rust
#[tokio::test]
async fn test_parallel_reconcile_prevents_premature_abort() {
    // Setup: Parallel(2 tasks), max_failures=1
    // 子-0 失败 → failed_count=1 → apparently_abort
    // 但子-0 已被重试（DB 中是 Running）
    // → reconcile 发现 actual_running=1 → 阻止 abort → Await
    
    let (node_instance, workflow_instance) = setup_parallel_with_one_failed();
    let reconciler = mock_reconciler(vec![
        ("child-0", TaskInstanceStatus::Running), // 被重试了
        ("child-1", TaskInstanceStatus::Completed),
    ]);
    let plugin = ParallelPlugin::new().with_reconciler(reconciler);
    
    let result = plugin.handle_callback(
        &mock_executor(),
        &mut node_instance,
        &mut workflow_instance,
        "child-1", // 第二个子任务完成
        &NodeExecutionStatus::Success,
        &None, &None, &None,
    ).await.unwrap();
    
    // 不应该 abort，应该继续 Await
    assert_eq!(result.status, NodeExecutionStatus::Await);
}

#[tokio::test]
async fn test_parallel_reconcile_confirms_real_failure() {
    // Setup: 计数器说 all_done with failures
    // reconcile 确认确实全部是 Failed → 正常 Failed
    
    let reconciler = mock_reconciler(vec![
        ("child-0", TaskInstanceStatus::Completed),
        ("child-1", TaskInstanceStatus::Failed),
    ]);
    // ... 验证返回 Failed
}
```

---

## 12. 迁移计划

### 12.1 阶段 1：添加基础设施

1. 新建 `src/crates/domain/src/plugin/reconciler.rs`
2. 在 `TaskInstanceService` 添加 `batch_get_statuses`
3. 在 Repository trait 添加 `batch_get_statuses`
4. 在 MongoDB 实现中实现 `$in` 查询

### 12.2 阶段 2：注入到容器插件

1. ParallelPlugin 添加 `reconciler` 字段
2. ForkJoinPlugin 添加 `reconciler` 字段
3. engine.rs 构造时注入

### 12.3 阶段 3：在 handle_callback 中使用

1. Parallel handle_callback 终态决策点调用 reconcile
2. ForkJoin handle_callback 终态决策点调用 reconcile
3. 保留现有 Stale Check（共存）

### 12.4 阶段 4：测试验证

1. ContainerReconciler 单元测试
2. Parallel/ForkJoin 集成测试
3. 端到端测试：重试场景

---

## 13. 未来扩展

### 13.1 支持子工作流（Phase 2 of ForkJoin）

当 ForkJoin 支持 SubWorkflow 子任务时：

```rust
impl ContainerReconciler {
    pub async fn reconcile_workflow_children(
        &self,
        child_workflow_ids: &[String],
    ) -> anyhow::Result<ReconcileResult> {
        // 查询 workflow_instances 集合
    }
    
    /// 混合查询：支持 Task + Workflow 子任务
    pub async fn reconcile_mixed_children(
        &self,
        task_ids: &[String],
        workflow_ids: &[String],
    ) -> anyhow::Result<ReconcileResult> {
        let task_result = self.reconcile_task_children(task_ids).await?;
        let wf_result = self.reconcile_workflow_children(workflow_ids).await?;
        Ok(task_result.merge(wf_result))
    }
}
```

### 13.2 替代 Stale Check（远期）

验证 reconcile 稳定后，可以：
1. 移除 Stale Failure Check 逻辑
2. 移除 `PluginExecutor::is_task_still_failed`
3. 计数器降级为"快照/缓存"

### 13.3 Sweeper 简化（远期）

Sweeper 的 `recover_await_container` 可以简化为：
- 对卡住的容器节点发一个"虚拟 callback"
- handle_callback 中的 reconcile 自动修正状态并做正确决策
- 不再需要 Sweeper 遍历所有子任务逐一补发

---

## 14. 和 EBUSY 方案的实施顺序

```
1. 实施 EBUSY (409) ← 解决当前 bug
2. 实施 ContainerReconciler ← 增强正确性
3. 实施批量重试 API ← 优化 UX（可选）
```

EBUSY 和 ContainerReconciler 是独立的，可以并行实施。

---

## 15. 文件变更清单

| 文件 | 变更 |
|------|------|
| `domain/plugin/reconciler.rs`（新建）| ContainerReconciler + ReconcileResult |
| `domain/plugin/mod.rs` | 新增 `pub mod reconciler` |
| `domain/task/service.rs` | 新增 `batch_get_statuses` |
| `domain/task/repository.rs`（trait）| 新增 `batch_get_statuses` |
| `infrastructure/.../task_repo.rs` | MongoDB `$in` 查询实现 |
| `domain/plugin/plugins/parallel.rs` | 注入 reconciler + 终态 reconcile |
| `domain/plugin/plugins/forkjoin.rs` | 注入 reconciler + 终态 reconcile |
| `src/bin/engine.rs` | 构造 reconciler 并注入 |
