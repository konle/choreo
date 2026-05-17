# Workflow Engine

基于 Rust 构建的分布式工作流引擎，采用编排器（Orchestrator）与执行器（Executor）分离架构，支持多租户、插件化任务系统和可视化 DAG 编排。

## 系统架构

```
┌──────────┐   HTTP    ┌──────────┐   Queue    ┌───────────────┐
│ Frontend │ ───────── │ APIServer│ ────────── │ Workflow Worker│
│ (Vue 3)  │   /api    │ (Axum)   │  (Redis)   │ (编排器)       │
└──────────┘           └──────────┘            └──────┬────────┘
                            │                         │ dispatch
                            │ MongoDB                 ▼
                            ▼                  ┌──────────────┐
                       ┌─────────┐             │ Task Worker   │
                       │ MongoDB │◄────────────│ (执行器)      │
                       └─────────┘   callback  └──────────────┘
```

| 组件 | 技术栈 | 说明 |
|------|--------|------|
| APIServer | Rust / Axum | REST API，JWT 认证，RBAC 权限 |
| Workflow Worker | Rust / Apalis | 工作流 DAG 编排、状态机流转 |
| Task Worker | Rust / Apalis | 原子任务执行（HTTP、gRPC 等） |
| Frontend | Vue 3 / TypeScript / Arco Design | 可视化管理与 DAG 编排器 |
| 数据库 | MongoDB | 工作流/任务/用户等持久化存储 |
| 消息队列 | Redis (Apalis) | Worker 间任务分发与事件回调 |

## 核心特性

- **多租户隔离** — 数据层原生 `tenant_id` 隔离，RBAC 五角色权限体系
- **插件化任务** — PluginInterface（编排）+ TaskExecutor（执行）双层抽象
- **并发容器** — Parallel（同构数据并行）、ForkJoin（异构任务并行），Scatter-Gather 增量状态机
- **子工作流嵌套** — 任意工作流可作为另一个工作流的节点，支持深度限制
- **变量系统** — 四层优先级覆盖（租户 → 模板 → 实例 → 节点），Secret 加密存储
- **分布式一致性** — 乐观锁（epoch CAS）+ 租约（Lease）防止并发回调数据撕裂
- **可视化编排** — 拖拽式 DAG 画布，8 种节点类型，实例运行时实时着色

## 环境要求

| 依赖 | 版本 |
|------|------|
| Rust | 1.85+ |
| Node.js | 20+ |
| Docker & Docker Compose | 最新稳定版 |

## 快速开始

### 1. 克隆仓库

```bash
git clone <repo-url>
cd workflow
```

### 2. 本地开发

本地开发模式下，MongoDB 和 Redis 通过 Docker 运行，Rust 服务和前端在本机直接运行。

```bash
# 启动基础设施（MongoDB + Redis）
make dev

# 终端 1: 启动 API Server (localhost:3000)
make run-api

# 终端 2: 启动工作流引擎 (消费队列)
make run-engine

# 终端 3: 启动前端开发服务器 (localhost:5173)
make install-frontend   # 首次需要安装依赖
make run-frontend
```

打开浏览器访问 `http://localhost:5173`，前端 Vite dev server 自动将 `/api` 请求代理到 `localhost:3000`。

### 3. 生产部署（Docker Compose 一键启动）

```bash
# 构建所有镜像（后端 + 前端）
make build

# 启动全部服务
make up

# 查看日志
make logs
```

部署后访问 `http://localhost`（默认 80 端口），nginx 托管前端静态文件并反向代理 API 请求。

### 4. 停止服务

```bash
# 停止本地基础设施
make dev-down

# 停止生产环境
make down

# 清理所有（含数据卷）
make clean
```

## Makefile 命令速查

| 命令 | 说明 |
|------|------|
| `make dev` | 启动 MongoDB + Redis（本地开发） |
| `make dev-down` | 停止本地基础设施 |
| `make run-api` | 运行 API Server |
| `make run-engine` | 运行工作流引擎 |
| `make run-frontend` | 运行前端 dev server |
| `make install-frontend` | 安装前端 npm 依赖 |
| `make build-frontend` | 构建前端生产产物 |
| `make build` | Docker 构建全部镜像 |
| `make up` | Docker Compose 启动全部服务 |
| `make down` | Docker Compose 停止全部服务 |
| `make logs` | 查看容器日志 |
| `make check` | Rust 类型检查 |
| `make clean` | 清理构建产物和数据卷 |

## 环境变量

复制 `deploy/.env.example` 为 `deploy/.env` 进行自定义：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MONGO_URL` | `mongodb://mongodb:27017` | MongoDB 连接地址 |
| `REDIS_URL` | `redis://redis:6379` | Redis 连接地址 |
| `API_PORT` | `3000` | API Server 监听端口 |
| `FRONTEND_PORT` | `80` | 前端（nginx）对外端口 |

## 项目结构

```
workflow/
├── src/
│   ├── bin/
│   │   ├── apiserver.rs              # API Server 入口
│   │   └── engine.rs                 # 工作流引擎入口
│   └── crates/
│       ├── api/                      # HTTP 层：路由、Handler、中间件
│       ├── domain/                   # 领域层：实体、服务、插件、执行器
│       └── infrastructure/           # 基础设施层：MongoDB 实现
├── frontend/                         # 前端项目
│   ├── src/
│   │   ├── api/                      # API 请求模块
│   │   ├── components/               # 布局与通用组件
│   │   ├── composables/              # 可复用逻辑
│   │   ├── router/                   # 路由与守卫
│   │   ├── stores/                   # Pinia 状态管理
│   │   ├── types/                    # TypeScript 类型定义
│   │   ├── utils/                    # 工具函数
│   │   └── views/                    # 页面视图
│   │       ├── auth/                 # 登录 / 注册
│   │       ├── dashboard/            # 仪表盘
│   │       ├── task/                 # 任务模板 / 实例
│   │       ├── workflow/             # 工作流管理 / 实例 / 可视化编排器
│   │       ├── variable/             # 变量管理
│   │       ├── tenant/               # 租户管理 (SuperAdmin)
│   │       └── user/                 # 用户管理 (TenantAdmin+)
│   └── package.json
├── deploy/
│   ├── docker-compose.yml            # 生产部署
│   ├── docker-compose.dev.yml        # 本地开发基础设施
│   ├── Dockerfile                    # 后端镜像
│   ├── Dockerfile.frontend           # 前端镜像
│   ├── nginx.conf                    # nginx 配置
│   └── .env.example                  # 环境变量模板
├── docs/
│   ├── architecture.md               # 后端架构设计文档
│   └── frontend-architecture.md      # 前端架构设计文档
├── Cargo.toml
├── Makefile
└── README.md
```

## API 路由概览

所有接口前缀 `/api/v1`，认证接口无需 JWT，其余接口需 `Authorization: Bearer <token>`。

```
POST   /auth/login                                    # 登录
POST   /auth/register                                 # 注册

CRUD   /tenants                                       # 租户管理 (SuperAdmin)
CRUD   /users                                         # 用户角色管理 (TenantAdmin+)

CRUD   /task                                          # 原子任务模板
CRUD   /task/instance                                 # 任务实例 + execute/retry/cancel
POST   /task/instance/{id}/execute

CRUD   /workflow/meta                                 # 工作流 Meta
CRUD   /workflow/meta/{id}/template                   # 版本化工作流模板
CRUD   /workflow/meta/{id}/variables                  # 模板级变量

CRUD   /workflow/instance                             # 工作流实例
POST   /workflow/instance/{id}/execute|cancel|retry|resume

CRUD   /variables                                     # 租户级变量
```

## 文档

- [后端架构设计](docs/architecture.md) — 插件系统、并发容器、分布式锁、多租户、变量系统
- [前端架构设计](docs/frontend-architecture.md) — 技术选型、页面设计、可视化编排器、权限体系
- [功能展示](docs/gallery.md) — 截图与使用演示
