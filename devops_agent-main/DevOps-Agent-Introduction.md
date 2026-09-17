# AWS DevOps Agent — Customer Introduction

## The Problem

Your operations team faces this every day:

- An alarm fires at 2 AM — an engineer spends 30–60 minutes just figuring out *what happened*
- Multiple alarms from the same root cause create noise and confusion
- Context gets lost between the person investigating and the person fixing
- The same class of issue recurs because post-incident improvements get deprioritized
- Creating an AWS Support case means re-explaining what you already investigated

**The result:** High MTTR, alert fatigue, and talented engineers doing repetitive investigation work instead of building.

---

## What AWS DevOps Agent Does

AWS DevOps Agent is an AI-powered operations agent that **investigates incidents autonomously** — the moment an alarm fires, not when a human wakes up. It correlates telemetry, checks deployment history, maps blast radius, and delivers a structured root cause analysis — typically in **under 2 minutes**.

It then goes further: proactively analyzing patterns across past incidents to recommend systemic improvements before the next outage happens.

---

## See It in Action: Real Investigation

**Scenario:** An EBS volume repeatedly exceeded its 125 MiB/s throughput limit — 6 alarms in 6 days.

**What the Agent did (zero human intervention):**

1. Spawned parallel sub-agents — one checked CloudTrail, another analyzed instance metrics
2. Identified pure read-only I/O bursts saturating the volume
3. Confirmed via CloudTrail that no AWS API calls triggered the reads
4. Correlated CPU and network metrics — CPU correlated with I/O, no network egress spike

**Root Cause:** A scheduled OS-level cron job was periodically reading the entire 10 GB volume at ~125 MiB/s, completing in 1–4 minutes each time.

**Recommendation:** Inspect OS-level crontab via SSM Session Manager; optimize the process or increase gp3 provisioned throughput.

> ⏱️ Total time: **under 2 minutes**, fully autonomous.

### Demo: Alarm-Triggered Investigation

案例：
週一晚，有一個磁盤的吞吐徒增， 觸發告警

![](./files/Capture-2026-07-08_17-19-09.jpeg)

自動方案由告警事件觸發 DevOps Agent 進行分析， 並將開始分析的事件通知 Telegram 或者 Slack
在 Slack 中的通知可以顯示具體的分析狀態及進展

![](./files/Capture-2026-07-08_17-21-04.jpeg)

![](./files/Capture-2026-07-08_17-29-38.jpeg)

We can also see the investigation details from the Web interface:

![](./files/Capture-2026-07-08_17-15-37.jpg)

---

## Key Capabilities

### 1. Automated Incident Investigation

Alarm fires → Agent immediately investigates → correlates metrics, logs, traces, CloudTrail, and deployment history → produces structured report with root cause, evidence, and recommended mitigations.

### 2. Proactive Prevention

Weekly automated evaluations analyze patterns across past **investigations**. Generates actionable recommendations in four categories: **Observability**, **Infrastructure**, **Governance**, and **Code optimization**. Recommendations come as agent-ready specs that can be handed to coding agents for implementation.

![](./files/Capture-2026-07-08_17-36-30.jpg)

### 3. Slack & AWS Support Integration

- Real-time investigation updates posted to your Slack channels — key findings, root cause analyses, and mitigation plans delivered as they happen
- Triage notifications in Slack — when incidents are linked or skipped, Slack receives correlation reasoning so the team stays informed without opening the console
- Create AWS Support cases directly from the web app with full investigation context (timeline, metrics, logs, resource info, remediation attempts) auto-attached — no re-explaining to support engineers

All team members will know the status, details shows in the Slack channel:

![](./files/Capture-2026-07-08_17-41-50.jpg)

Click to open support case and will include the details from investigation:

![](./files/Capture-2026-07-08_17-38-14.jpeg)

![](./files/Capture-2026-07-08_17-39-37.jpeg)

### 4. Kubernetes-Native Intelligence (EKS)

Understands Pod → Deployment → Service → Node relationships. Investigates crash loops, HPA failures, node pressure, service-to-service latency, and deployment rollout issues.

![](./files/Capture-2026-07-08_17-51-55.jpeg)

### 5. Multi-Language Support (including Chinese)

Agent Space responses and AWS Support case routing support Chinese (中文), English, French, Japanese, Korean, Portuguese, and Spanish. Configure via **Agent response language** in your Agent Space settings — investigations, root cause analyses, and support cases will be delivered in your preferred language and routed to language-matched support engineers.

![](./files/Capture-2026-07-08_18-04-59.jpg)

![](./files/Capture-2026-07-08_18-05-10.jpg)

### 6. Skills — Extend the Agent with Domain Knowledge

Skills are modular instruction sets that extend the agent's capabilities with specialized investigation methodologies tailored to your infrastructure. You can create skills in the UI, upload as zip, or import from GitHub repositories.

Community skills available at: https://github.com/aws-samples/sample-devops-agent-tools

![](./files/Capture-2026-07-08_18-26-34.jpg)

use the skills:

![](./files/Capture-2026-07-08_18-29-55.jpg)

![](./files/Capture-2026-07-09_12-03-15.jpg)

---

## How It Works

![Architecture](DevOps%20Agent.drawio.png)

```
Alarm/Webhook/ServiceNow → DevOps Agent investigates → Findings to Slack
                                                     → Optional: Create AWS Support case (context auto-attached)
                                                     → Weekly: Proactive recommendations
```

**Investigation triggers:**

| Source | How |
|--------|-----|
| CloudWatch Alarms | Automatic on ALARM state |
| Webhooks | Grafana, Prometheus, PagerDuty POST to Agent Space URL |
| ServiceNow | Automatic from support tickets via built-in integration |
| EventBridge | Rules to monitor events and trigger investigations |
| Manual | Describe symptoms in natural language via web app |

**Communication:** Slack receives real-time updates, triage decisions, and mitigation plans as investigations progress.

**Noise reduction:** Multiple alarms from the same root cause are automatically correlated into a single investigation.

### EventBridge Integration

Use EventBridge rules to monitor events and trigger investigations, or send notifications to Telegram/other channels:

![](./files/Capture-2026-07-08_18-08-30.jpeg)

![](./files/Capture-2026-07-08_18-10-43.jpg)

![](./files/Capture-2026-07-08_18-10-57.jpeg)

![](./files/Capture-2026-07-08_18-21-43.jpg)

![](./files/Capture-2026-07-08_18-20-56.jpeg)

---

## Setup & Configuration

### What You Need to Get Started

| Requirement | Details |
|-------------|---------|
| **AWS Account(s)** | Where your application resources run |
| **IAM Role** | `AIOpsAssistantPolicy` for agent read access |
| **CloudWatch + CloudTrail** | Minimum observability (already enabled in most accounts) |
| **Optional** | GitHub/GitLab for deployment correlation; APM tools (Datadog, Dynatrace, etc.) for richer context |

### Access Control

Grant users/groups from IAM Identity Center for access to the Web Interface:

![](./files/Capture-2026-07-08_17-59-38.jpeg)

### EKS Access Configuration

Add corresponding Cloud Account resources. The role here is the identity used to run investigations — keep read privilege rather than write.

![](./files/Capture-2026-07-08_17-56-43.jpg)

On the EKS Cluster side:

![](./files/Capture-2026-07-08_18-00-44.jpeg)

### GitHub Integration (Two-Step Process)

GitHub integration follows a two-step process — a common gotcha during setup:

1. **Account-level registration** — Register GitHub under **Capability Providers** in the DevOps Agent console. This installs the GitHub App and authorizes access to your repos. Supports GitHub.com and GitHub Enterprise Server.
2. **Agent Space association** — Connect specific repositories to your Agent Space via **Capabilities > Pipeline > Add > GitHub**. Select your registration and choose which repos this Agent Space can access.

> ⚠️ Without step 2, you'll get errors like *"No GitHub association is configured for this agent space"* when trying to import skills from a repository.

**What GitHub enables:**
- Deployment correlation during incident investigations (code changes → operational impact)
- Import skills directly from GitHub repos (version-controlled, syncable)
- Release readiness code reviews on pull requests (auto-triggered)
- Automated verification testing in managed environments

![](./files/Capture-2026-07-08_18-24-17.jpg)

---

## Supported Integrations

| Category | Tools |
|----------|-------|
| **Observability** | CloudWatch, Datadog, Dynatrace, New Relic, Splunk, Grafana |
| **Source Control** | GitHub, GitLab, Azure DevOps |
| **Incident Management** | PagerDuty, ServiceNow |
| **Communication** | Slack, Microsoft Teams |
| **Extensibility** | MCP servers, Webhooks, ACP, A2A protocols |

---

## Benefits

| | |
|---|---|
| **MTTR** | Investigation starts in seconds, not hours |
| **Prevention** | Weekly evaluations catch systemic issues before they cause outages |
| **Support Speed** | Cases pre-loaded with full context — faster resolution |
| **Team Focus** | Engineers build instead of investigating repetitive incidents |

---

## Next Steps

1. **PoC Setup** — I can help configure an Agent Space in your staging/production account (~1-2 hours)
2. **Observation Period** — Let DevOps Agent run for 1 week against your existing alarms
3. **Review** — Walk through investigation results and proactive recommendations together
4. **Expand** — Add integrations (Slack, GitHub, APM tools) and extend to additional accounts

---

## Learn More

- [Documentation](https://docs.aws.amazon.com/devopsagent/latest/userguide/about-aws-devops-agent.html)
- [Production Best Practices](https://aws.amazon.com/blogs/devops/best-practices-for-deploying-aws-devops-agent-in-production/)
- [Multi-Agent Reasoning Deep Dive](https://aws.amazon.com/blogs/devops/how-aws-devops-agent-uses-multi-agent-reasoning-to-find-root-causes/)
- [EKS Integration](https://aws.amazon.com/blogs/architecture/ai-powered-event-response-for-amazon-eks/)
- [CDK Sample](https://github.com/aws-samples/sample-aws-devops-agent-cdk) | [Terraform Sample](https://github.com/aws-samples/sample-aws-devops-agent-terraform)

---

*Prepared by your AWS Technical Account Manager*
