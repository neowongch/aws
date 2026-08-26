# DevOps Agent — 告警自动触发调查 部署指南

## 概述

本 CloudFormation 模板可一键部署完整的 AWS DevOps Agent 告警自动调查方案。当 CloudWatch 告警进入 ALARM 状态时，系统将自动触发 DevOps Agent 进行根因分析。

### 架构图

```
CloudWatch Alarm (进入 ALARM 状态)
    → EventBridge 规则 (捕获告警状态变化)
        → Lambda 函数 (devops-webhook-executor)
            → 从 Secrets Manager 读取 Webhook 凭证
            → 发送 HMAC-SHA256 签名请求至 DevOps Agent Webhook
                → DevOps Agent 自动启动调查
```

### 部署资源清单

| # | 资源 | 用途 |
|---|------|------|
| 1 | IAM Identity Center 自动发现 | 自动获取 IDC 实例 ARN |
| 2 | IAM 角色 (Agent Space) | 授权 DevOps Agent 访问账户资源 |
| 3 | IAM 角色 (Operator App) | Web App 访问角色 (支持 IAM + IDC 登录) |
| 4 | Agent Space | DevOps Agent 工作空间 |
| 5 | AWS 账户关联 | 将部署账户设为监控账户 |
| 6 | Event Channel + Webhook | 自动注册 Webhook 并生成 HMAC 密钥 |
| 7 | Secrets Manager | 存储 Webhook URL 和 HMAC 密钥 (自动填充) |
| 8 | Lambda 函数 | 格式化告警事件并发送签名 Webhook 请求 |
| 9 | EventBridge 规则 | 捕获所有 CloudWatch 告警状态变化 |

---

## 前提条件

1. AWS 账户具有创建 IAM 角色、Lambda、EventBridge、Secrets Manager 和 DevOps Agent 资源的权限
2. 组织中已配置 **IAM Identity Center** (用于 SSO 登录 Web App)
3. 部署区域：**us-east-1**
4. 已配置 AWS CLI

---

## 部署步骤

### 第一步：部署 CloudFormation Stack

```bash
aws cloudformation deploy \
  --template-file cfn-devops-alarm-auto-investigation.yaml \
  --stack-name devops-alarm-investigation \
  --parameter-overrides AgentSpaceName="<你的Agent Space名称>" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

**参数说明：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `AgentSpaceName` | `DevOpsAgentSpace` | Agent Space 名称 |
| `AgentSpaceDescription` | 自动配置... | Agent Space 描述 |

部署时间约 3-5 分钟。全程自动化，无需手动配置基础设施。

### 第二步：确认部署成功

```bash
aws cloudformation describe-stacks \
  --stack-name devops-alarm-investigation \
  --query 'Stacks[0].Outputs' \
  --region us-east-1 \
  --output table
```

确认以下输出：
- `AgentSpaceId` — Agent Space ID
- `IdcInstanceArn` — 自动发现的 IAM Identity Center 实例
- `WebhookId` — 自动生成的 Webhook ID
- `OperatorWebAppUrl` — DevOps Agent 控制台 URL

---

## 授权用户访问

部署完成后，需要授权用户访问 DevOps Agent Web App。需完成以下两个步骤：

### 步骤 A：在 IAM Identity Center 中分配用户

1. 打开 AWS 控制台 → **IAM Identity Center**
2. 左侧菜单选择 **Applications** (应用程序)
3. 找到名为 **DevOps Agent** 的应用 (类型为 `aidevops.amazonaws.com`)
4. 点击该应用 → **Assign users and groups** (分配用户和组)
5. 选择需要访问的用户或组 → **Assign** (分配)



### 步骤 B：在 DevOps Agent 控制台中授权

1. 打开 DevOps Agent 控制台 (使用 Stack 输出的 `OperatorWebAppUrl`)
2. 进入 Agent Space → **Settings** (设置) → **User Access** (用户访问)
3. 点击 **Add user** (添加用户)
4. 选择用户并分配角色：
   - **Operator** — 可以查看调查结果、触发调查、管理配置
   - **Viewer** — 只能查看调查结果


---

## 测试告警触发

### 创建测试告警并触发

```bash
# 创建测试告警
aws cloudwatch put-metric-alarm \
  --alarm-name "DevOpsAgent-Test-Alarm" \
  --metric-name CPUUtilization \
  --namespace AWS/EC2 \
  --statistic Average \
  --period 60 \
  --threshold 80 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1 \
  --dimensions Name=InstanceId,Value=i-test12345 \
  --region us-east-1

# 触发 ALARM 状态
aws cloudwatch set-alarm-state \
  --alarm-name "DevOpsAgent-Test-Alarm" \
  --state-value ALARM \
  --state-reason "测试 DevOps Agent 自动调查" \
  --region us-east-1
```

### 验证 Lambda 执行

```bash
aws logs tail "/aws/lambda/devops-webhook-executor-devops-alarm-investigation" \
  --since 2m \
  --region us-east-1
```

预期输出：
```
INFO  WebHook Executor invoked
INFO  Sending webhook: { url: 'https://event-ai.us-east-1.api.aws/webhook/generic/...', payloadSize: 677 }
INFO  Response: { status: 200, body: '{"message": "Webhook received"}' }
```

### 在控制台中查看调查

登录 DevOps Agent Web App，在 Agent Space 中可以看到新创建的调查任务，包含告警详情和自动分析结果。

---

## 清理测试资源

```bash
# 删除测试告警
aws cloudwatch delete-alarms \
  --alarm-names "DevOpsAgent-Test-Alarm" \
  --region us-east-1
```


---

*作者：Neo Wong (wangneo) — TAM, APJC-GCR*
*日期：2026-07-22*
