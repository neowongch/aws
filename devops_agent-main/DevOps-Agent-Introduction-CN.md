# AWS DevOps Agent — 客户介绍

## 痛点

您的运维团队每天都面临这些挑战：

- 凌晨 2 点告警触发 — 工程师花 30–60 分钟仅仅是弄清楚*发生了什么*
- 同一根因引发多个告警，产生噪音和混乱
- 调查人员和修复人员之间上下文丢失
- 同类问题反复出现，因为事后改进总被降低优先级
- 创建 AWS Support 工单意味着重新解释你已经调查过的内容

**结果：** MTTR 居高不下、告警疲劳、优秀工程师做着重复的调查工作而非专注于构建。

---

## AWS DevOps Agent 做什么

AWS DevOps Agent 是一个 AI 驱动的运维代理，能够**自主调查事件** — 在告警触发的那一刻就开始，而不是等人醒来。它关联遥测数据、检查部署历史、映射影响范围，并交付结构化的根因分析 — 通常在 **2 分钟内**完成。

它还能更进一步：主动分析历史事件模式，在下一次故障发生之前推荐系统性改进。

---

## 实际案例：真实调查

**场景：** 一个 EBS 卷反复超出 125 MiB/s 吞吐量限制 — 6 天内触发 6 次告警。

**Agent 的操作（零人工干预）：**

1. 生成并行子代理 — 一个检查 CloudTrail，另一个分析实例指标
2. 识别出纯只读 I/O 突发使卷饱和
3. 通过 CloudTrail 确认没有 AWS API 调用触发了这些读取
4. 关联 CPU 和网络指标 — CPU 与 I/O 相关，无网络出口流量峰值

**根因：** 一个定时的 OS 级 cron 任务周期性地以 ~125 MiB/s 读取整个 10 GB 卷，每次耗时 1–4 分钟。

**建议：** 通过 SSM Session Manager 检查 OS 级 crontab；优化该进程或增加 gp3 预置吞吐量。

> ⏱️ 总耗时：**不到 2 分钟**，完全自主。

### 演示：告警触发的自动调查

案例：
週一晚，有一個磁盤的吞吐徒增， 觸發告警

![](./files/Capture-2026-07-08_17-19-09.jpeg)

自動方案由告警事件觸發 DevOps Agent 進行分析， 並將開始分析的事件通知 Telegram 或者 Slack
在 Slack 中的通知可以顯示具體的分析狀態及進展

![](./files/Capture-2026-07-08_17-21-04.jpeg)

![](./files/Capture-2026-07-08_17-29-38.jpeg)

也可以通过 Web 界面查看调查详情：

![](./files/Capture-2026-07-08_17-15-37.jpg)

---

## 核心能力

### 1. 自动化事件调查

告警触发 → Agent 立即调查 → 关联指标、日志、链路追踪、CloudTrail 和部署历史 → 生成包含根因、证据和建议缓解措施的结构化报告。

### 2. 主动预防

每周自动评估，分析历史**调查**中的模式。生成四个类别的可操作建议：**可观测性**、**基础设施**、**治理**和**代码优化**。建议以代理可执行的规范形式呈现，可交由编码代理实施。

![](./files/Capture-2026-07-08_17-36-30.jpg)

### 3. Slack 与 AWS Support 集成

- 实时调查更新推送到 Slack 频道 — 关键发现、根因分析和缓解方案实时推送
- 分流通知在 Slack 中展示 — 当事件被关联或跳过时，Slack 收到关联原因，团队无需打开控制台即可了解状态
- 从 Web 应用直接创建 AWS Support 工单，完整调查上下文（时间线、指标、日志、资源信息、缓解尝试）自动附加 — 无需重新向支持工程师解释

所有团队成员都能在 Slack 频道中看到状态和详情：

![](./files/Capture-2026-07-08_17-41-50.jpg)

点击创建支持工单，将自动包含调查详情：

![](./files/Capture-2026-07-08_17-38-14.jpeg)

![](./files/Capture-2026-07-08_17-39-37.jpeg)

### 4. Kubernetes 原生智能（EKS）

理解 Pod → Deployment → Service → Node 关系。调查崩溃循环、HPA 故障、节点压力、服务间延迟和部署滚动更新问题。

![](./files/Capture-2026-07-08_17-51-55.jpeg)

### 5. 多语言支持（支持中文）

Agent Space 响应和 AWS Support 工单路由支持中文、英文、法文、日文、韩文、葡萄牙文和西班牙文。通过 Agent Space 设置中的 **Agent response language** 配置 — 调查结果、根因分析和支持工单将以您的首选语言交付，并路由到对应语言的支持工程师。

![](./files/Capture-2026-07-08_18-04-59.jpg)

![](./files/Capture-2026-07-08_18-05-10.jpg)

### 6. Skills — 用领域知识扩展 Agent

Skills 是模块化的指令集，用于为 Agent 提供针对您基础设施的专业调查方法。您可以在 UI 中创建、以 zip 上传，或从 GitHub 仓库导入。

社区 Skills 仓库：https://github.com/aws-samples/sample-devops-agent-tools

![](./files/Capture-2026-07-08_18-26-34.jpg)

使用 Skills：

![](./files/Capture-2026-07-08_18-29-55.jpg)

![](./files/Capture-2026-07-09_12-03-15.jpg)

---

## 工作原理

![架构图](DevOps%20Agent.drawio.png)

```
告警/Webhook/ServiceNow → DevOps Agent 调查 → 发现推送到 Slack
                                             → 可选：创建 AWS Support 工单（上下文自动附加）
                                             → 每周：主动预防建议
```

**调查触发方式：**

| 来源 | 方式 |
|------|------|
| CloudWatch 告警 | ALARM 状态时自动触发 |
| Webhooks | Grafana、Prometheus、PagerDuty POST 到 Agent Space URL |
| ServiceNow | 通过内置集成从支持工单自动触发 |
| EventBridge | 通过规则监控事件并触发调查 |
| 手动 | 在 Web 应用中用自然语言描述症状 |

**通信：** Slack 实时接收更新、分流决策和缓解方案。

**降噪：** 来自同一根因的多个告警自动关联为单次调查。

### EventBridge 集成

使用 EventBridge 规则监控事件并触发调查，或发送通知到 Telegram 等渠道：

![](./files/Capture-2026-07-08_18-08-30.jpeg)

![](./files/Capture-2026-07-08_18-10-43.jpg)

![](./files/Capture-2026-07-08_18-10-57.jpeg)

![](./files/Capture-2026-07-08_18-21-43.jpg)

![](./files/Capture-2026-07-08_18-20-56.jpeg)

---

## 设置与配置

### 开始所需条件

| 要求 | 详情 |
|------|------|
| **AWS 账户** | 应用资源运行的位置 |
| **IAM 角色** | `AIOpsAssistantPolicy` 提供 Agent 只读访问 |
| **CloudWatch + CloudTrail** | 最低可观测性要求（大多数账户已启用） |
| **可选** | GitHub/GitLab 用于部署关联；APM 工具（Datadog、Dynatrace 等）提供更丰富的上下文 |

### 访问控制

通过 IAM Identity Center 授予用户/组访问 Web 界面的权限：

![](./files/Capture-2026-07-08_17-59-38.jpeg)

### EKS 访问配置

添加对应的 Cloud Account 资源。此处的角色是用于运行调查的身份 — 保持只读权限而非写入权限。

![](./files/Capture-2026-07-08_17-56-43.jpg)

在 EKS 集群侧：

![](./files/Capture-2026-07-08_18-00-44.jpeg)

### GitHub 集成（两步流程）

GitHub 集成遵循两步流程 — 设置时的常见坑：

1. **账户级注册** — 在 DevOps Agent 控制台的 **Capability Providers** 下注册 GitHub。这会安装 GitHub App 并授权访问您的仓库。支持 GitHub.com 和 GitHub Enterprise Server。
2. **Agent Space 关联** — 通过 **Capabilities > Pipeline > Add > GitHub** 将特定仓库连接到您的 Agent Space。选择注册并选择此 Agent Space 可访问的仓库。

> ⚠️ 如果没有完成第 2 步，尝试从仓库导入 Skills 时会收到错误：*"No GitHub association is configured for this agent space"*

**GitHub 启用的功能：**
- 事件调查中的部署关联（代码变更 → 运营影响）
- 从 GitHub 仓库直接导入 Skills（版本控制、可同步）
- Pull Request 上的发布就绪代码审查（自动触发）
- 托管环境中的自动化验证测试

![](./files/Capture-2026-07-08_18-24-17.jpg)

---

## 支持的集成

| 类别 | 工具 |
|------|------|
| **可观测性** | CloudWatch、Datadog、Dynatrace、New Relic、Splunk、Grafana |
| **源代码管理** | GitHub、GitLab、Azure DevOps |
| **事件管理** | PagerDuty、ServiceNow |
| **通信** | Slack、Microsoft Teams |
| **可扩展性** | MCP 服务器、Webhooks、ACP、A2A 协议 |

---

## 收益

| | |
|---|---|
| **MTTR** | 调查在秒级开始，而非数小时 |
| **预防** | 每周评估在故障发生前捕获系统性问题 |
| **支持速度** | 工单预加载完整上下文 — 更快解决 |
| **团队专注** | 工程师专注于构建而非调查重复性事件 |

---

## 下一步

1. **PoC 搭建** — 我可以帮助在您的测试/生产账户中配置 Agent Space（约 1-2 小时）
2. **观察期** — 让 DevOps Agent 针对您现有的告警运行 1 周
3. **回顾** — 一起查看调查结果和主动预防建议
4. **扩展** — 添加集成（Slack、GitHub、APM 工具）并扩展到更多账户

---

## 了解更多

- [文档](https://docs.aws.amazon.com/devopsagent/latest/userguide/about-aws-devops-agent.html)
- [生产最佳实践](https://aws.amazon.com/blogs/devops/best-practices-for-deploying-aws-devops-agent-in-production/)
- [多代理推理深度解析](https://aws.amazon.com/blogs/devops/how-aws-devops-agent-uses-multi-agent-reasoning-to-find-root-causes/)
- [EKS 集成](https://aws.amazon.com/blogs/architecture/ai-powered-event-response-for-amazon-eks/)
- [CDK 示例](https://github.com/aws-samples/sample-aws-devops-agent-cdk) | [Terraform 示例](https://github.com/aws-samples/sample-aws-devops-agent-terraform)

---

