# AWS Lambda MicroVMs — AI Agent 安全执行沙盒

> 用 Serverless MicroVM 替代 EKS + Airflow，实现"代码管理一切，Agent 管理代码，人员管理 Agent"

---

## 当前挑战

构建 AI Agent 系统时，传统架构面临三个核心矛盾：

| 挑战 | 传统方案 (EKS + Airflow + Dify) | 问题 |
|------|-------------------------------|------|
| **安全隔离** | K8s Pod (namespace 隔离) | ❌ 共享 kernel，容器逃逸风险 |
| **代码化管理** | Airflow DAG YAML + Dify GUI | ❌ 配置驱动，非代码优先 |
| **运维负担** | EKS 集群升级 + 节点管理 | ❌ 基础设施运维占用工程时间 |
| **弹性与成本** | Node pool 预置 + Pod 持续运行 | ❌ 空闲也计费，扩缩容不够灵活 |

> **理想模式：** 代码管理一切 → Agent 管理代码 → 人员管理 Agent。
> 基础设施应该是透明的，Agent 应该在安全沙盒中自主执行。

---

## 解决方案：AWS Lambda MicroVMs

![Lambda MicroVM vs Traditional Always-On Model](https://raw.githubusercontent.com/aws-samples/sample-multi-tenant-ai-agents-on-lambda-microvm/main/docs/images/value-proposition.jpg)
*传统 always-on 模型 vs Lambda MicroVM 模型*

Lambda MicroVMs 是 AWS Lambda 平台上的新型 Serverless 计算原语，基于 **Firecracker** 虚拟化技术（支撑 AWS Lambda 每月超 15 万亿次调用的同一技术）。每个 MicroVM 提供：

### 核心能力

| 能力 | 说明 |
|------|------|
| 🔒 **VM 级隔离** | 独立 kernel、独立文件系统、独立网络命名空间。即使 Agent 生成恶意代码也无法逃逸沙盒。 |
| ⚡ **近即时启动** | 基于快照恢复，跳过 OS 启动和依赖初始化。启动延迟在秒级以下。 |
| 💾 **状态保持** | 内存和磁盘状态在请求间持续存在。支持自动 Suspend/Resume，idle 时低成本保持。 |
| 📈 **弹性伸缩** | 每分钟可启动数百个 MicroVM。按需创建，用完销毁。无需预置节点池。 |
| 🌐 **直连 HTTPS** | 每个 MicroVM 有独立 HTTPS 端点。支持 WebSocket、gRPC、SSE。无需配置负载均衡。 |
| 💰 **按用量计费** | 运行时按 vCPU-秒 + 内存-秒计费。Suspend 时极低成本保留状态。终止后零费用。 |

---


### 目标架构

```
┌─────────────────────────────────────────────────────────┐
│                    人员层 (管理 Agent)                     │
│         定义目标、设置策略 (Cedar Policy)、审核            │
├─────────────────────────────────────────────────────────┤
│                    Agent 层 (管理代码)                     │
│    LLM 推理决策 → 生成代码 → 发送执行 → 处理结果          │
│              所有逻辑以代码形式存在 (Git 管理)             │
├─────────────────────────────────────────────────────────┤
│                   执行层 (Lambda MicroVM)                  │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐           │
│  │ MicroVM A │  │ MicroVM B │  │ MicroVM C │           │
│  │ (Session1)│  │ (Session2)│  │ (Session3)│           │
│  │ 独立kernel │  │ 独立kernel │  │ 独立kernel │           │
│  └───────────┘  └───────────┘  └───────────┘           │
│         ↕ HTTPS         ↕ HTTPS         ↕ HTTPS        │
└─────────────────────────────────────────────────────────┘
```

### 隔离模型对比

```
  EKS Pod (当前):                    Lambda MicroVM (目标):
  ┌────────────────────────┐        ┌─────────┐ ┌─────────┐ ┌─────────┐
  │    共享 Linux Kernel    │        │Kernel A │ │Kernel B │ │Kernel C │
  │ ┌──────┐┌──────┐┌────┐│        │┌───────┐│ │┌───────┐│ │┌───────┐│
  │ │Agent1││Agent2││ .. ││        ││Agent A││ ││Agent B││ ││Agent C││
  │ └──────┘└──────┘└────┘│        │└───────┘│ │└───────┘│ │└───────┘│
  └────────────────────────┘        └─────────┘ └─────────┘ └─────────┘
     ↑ Namespace 隔离                  ↑ Firecracker VM 隔离
     (共享kernel, 可逃逸)               (独立kernel, 不可逃逸)
```

---

## 工作流程

1. **构建 MicroVM Image（一次性）** — 定义 Dockerfile，安装运行时和依赖。Lambda 构建并拍摄 Firecracker 快照。
2. **Agent 请求执行环境** — 调用 API 启动 MicroVM。从快照恢复，近即时可用。获得独立 HTTPS 端点。
3. **Agent 发送代码执行** — 通过 HTTPS POST 发送 LLM 生成的代码。MicroVM 执行并返回结果。状态在请求间保持。
4. **空闲自动 Suspend** — 配置时间内无流量，MicroVM 自动挂起（内存/磁盘状态保留）。成本降至最低。
5. **流量恢复自动 Resume** — 下次请求到达时自动唤醒。用户/Agent 继续上次的工作，无需重新初始化。
6. **Session 结束，销毁环境** — 任务完成后终止 MicroVM。零残留，零持续成本。下次按需再创建。

---

## 代码示例

### Agent 编排器调用 MicroVM 执行代码

```python
# Agent 生成代码后，发送到 MicroVM 执行
import requests

ENDPOINT = "https://<microvm-id>.lambda-microvm.us-east-1.on.aws"
TOKEN = "<auth-token>"

# Agent 生成的分析代码
agent_code = """
import pandas as pd
import numpy as np

df = pd.DataFrame({
    'trade_volume': np.random.randint(100, 10000, 50),
    'latency_ms': np.random.uniform(1, 50, 50)
})
print(df.describe())
print(f"Correlation: {df.corr().iloc[0,1]:.4f}")
"""

# 发送到 MicroVM 执行
response = requests.post(
    f"{ENDPOINT}/execute",
    headers={
        "X-aws-proxy-auth": TOKEN,
        "Content-Type": "application/json"
    },
    json={"code": agent_code}
)

result = response.json()
print(result["stdout"])  # Agent 获取执行结果
```

> 💡 **关键点：** 整个过程是标准 HTTPS 调用。无需特殊 SDK、无需 K8s 配置、无需 Airflow DAG。Agent 编排逻辑完全是代码，可以 Git 管理、可以 CI/CD、可以单元测试。

---

## 方案对比

| 维度 | Lambda MicroVM | EKS + Airflow |
|------|---------------|---------------|
| **启动速度** | ✅ <1s（快照恢复） | 分钟级（调度 → 拉镜像 → 初始化） |
| **隔离级别** | ✅ VM 级（独立 kernel） | Namespace 级（共享 kernel） |
| **状态保持** | ✅ 原生 Suspend/Resume | 需要外部存储（PV/S3/Redis） |
| **弹性** | ✅ 0→数百/分钟，无需预置 | Node Pool 预置 + HPA 调优 |
| **代码化程度** | ✅ 100% API 驱动 | YAML + Helm + GUI (Dify) |
| **运维负担** | ✅ 零基础设施管理 | 集群升级 / CNI / 调度器 / Worker 维护 |
| **成本模型** | ✅ 按秒计费 + idle 自动 suspend | 节点+Pod 持续运行计费 |
| **AI 代码安全** | ✅ 幻觉/注入代码无法逃逸 | 需要 PodSecurity + NetworkPolicy |

### 何时选择哪个方案

| 场景 | 推荐方案 |
|------|---------|
| AI Agent 代码执行（短任务、需隔离） | **Lambda MicroVM** |
| 交互式开发环境 / Notebook | **Lambda MicroVM** |
| 多租户 Agent 运行时 | **Lambda MicroVM** |
| 长运行有状态服务 (>8hr) | **EKS** |
| 复杂微服务架构 | **EKS** |
| 定时批处理管道 (ETL) | **Airflow + EKS** |
| 需要 GPU 的 ML 训练 | **EKS + GPU 节点** |

> 💡 Lambda MicroVM 和 EKS 不是互斥的。推荐：Agent 执行层用 MicroVM，长运行服务用 EKS。

---

## 适用场景

- **AI Agent 代码执行沙盒** — LLM 生成代码在隔离环境中安全执行，支持迭代（写→跑→修→跑）
- **多租户 Agent 运行时** — 每个 broker brand（TMGM、OQtima、HUBx）独立 MicroVM，零交叉污染
- **交互式开发环境** — 按需分配 IDE 环境，idle 自动挂起，回来即时恢复
- **数据分析 Notebook** — 加载大数据集，工作状态跨小时保持，无需重新计算
- **CI/CD 隔离构建** — 每次构建独立 VM，无共享状态，无缓存污染
- **安全扫描** — 漏洞评估在完全隔离环境中运行，支持提升权限操作

---

## 技术规格

| 参数 | 规格 |
|------|------|
| 基线配置 | 0.5 GB / 0.25 vCPU → 8 GB / 4 vCPU |
| 峰值突发 | 基线的 4 倍（自动弹性，按使用量计费） |
| 最大磁盘 | 8 GB – 32 GB（随配置而定） |
| 最大运行时间 | 8 小时 |
| 协议支持 | HTTPS, HTTP/2, WebSocket, gRPC, SSE |
| 网络 | 公网出站（默认）+ VPC 连接（Lambda Network Connector） |
| 架构 | ARM64 (Graviton) |
| 操作系统 | Amazon Linux 2023 |
| 可用区域 | us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1 |

---

## 下一步

1. **概念验证 (PoC)** — 基于当前 AI Agent 项目，在非生产环境搭建 MicroVM 沙盒，验证隔离性和性能。
2. **架构评审** — AWS SA 团队协助评审 Agent 编排架构，规划 MicroVM 与现有 EKS 工作负载的分工。
3. **逐步迁移** — 将 Agent 执行层从 EKS Pod 迁移到 MicroVM。EKS 保留用于长运行有状态服务。

---

## 参考资源

- [Lambda MicroVMs 产品页](https://aws.amazon.com/lambda/lambda-microvms/)
- [开发者文档](https://docs.aws.amazon.com/lambda/latest/dg/lambda-microvms-guide.html)
- [定价](https://aws.amazon.com/lambda/pricing/)
- [Secure code execution for AI agents (Blog)](https://aws.amazon.com/blogs/compute/secure-code-execution-for-ai-agents-with-aws-lambda-microvms/)
- [Multi-tenant AI agents sample (GitHub)](https://github.com/aws-samples/sample-multi-tenant-ai-agents-on-lambda-microvm)

---

*AWS Enterprise Support · Prepared by your Technical Account Manager*
