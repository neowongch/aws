# DevOps Agent — Alarm Auto-Trigger Investigation Deployment Guide

## Overview

This CloudFormation template deploys a fully automated solution that triggers AWS DevOps Agent investigations whenever a CloudWatch Alarm enters ALARM state. The deployment is a single-command operation with no manual infrastructure setup required.

### Architecture

```
CloudWatch Alarm (ALARM state)
    → EventBridge Rule (captures alarm state change)
        → Lambda Function (devops-webhook-executor)
            → Reads webhook credentials from Secrets Manager
            → Sends HMAC-SHA256 signed POST to DevOps Agent Webhook
                → DevOps Agent starts automated investigation
```

### What Gets Deployed

| # | Resource | Purpose |
|---|----------|---------|
| 1 | IAM Identity Center Discovery (Custom Resource) | Auto-discovers IDC instance ARN |
| 2 | IAM Role (Agent Space) | Grants DevOps Agent access to monitor account resources |
| 3 | IAM Role (Operator App) | Enables Web App access via IAM + IDC (with Trusted Identity Propagation) |
| 4 | Agent Space (`AWS::DevOpsAgent::AgentSpace`) | The DevOps Agent workspace with IAM + IDC operator app |
| 5 | AWS Account Association | Links the deployment account as monitor account |
| 6 | Event Channel + Webhook (Custom Resource) | Registers webhook service, generates URL + HMAC secret |
| 7 | Secrets Manager Secret | Stores webhook URL and HMAC secret (auto-populated) |
| 8 | Lambda Function (webhook executor) | Formats alarm event → sends signed webhook to DevOps Agent |
| 9 | EventBridge Rule | Captures `CloudWatch Alarm State Change → ALARM` events |
| 10 | Lambda Permission | Allows EventBridge to invoke the webhook executor |

---

## Prerequisites

1. **AWS Account** with permissions to create IAM roles, Lambda, EventBridge, Secrets Manager, and DevOps Agent resources
2. **IAM Identity Center** configured in the organization (for SSO-based web app access)
3. **Region**: Deploy in **us-east-1** (DevOps Agent supported region)
4. **AWS CLI** configured with appropriate credentials

---

## Deployment

### Step 1: Deploy the Stack

```bash
aws cloudformation deploy \
  --template-file cfn-devops-alarm-auto-investigation.yaml \
  --stack-name devops-alarm-investigation \
  --parameter-overrides AgentSpaceName="<YOUR_AGENT_SPACE_NAME>" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `AgentSpaceName` | `DevOpsAgentSpace` | Name for the Agent Space |
| `AgentSpaceDescription` | Auto-provisioned... | Description |

The deployment takes approximately 3-5 minutes.

### Step 2: Verify Deployment

```bash
aws cloudformation describe-stacks \
  --stack-name devops-alarm-investigation \
  --query 'Stacks[0].Outputs' \
  --region us-east-1 \
  --output table
```

Expected outputs:
- `AgentSpaceId` — The Agent Space identifier
- `IdcInstanceArn` — Auto-discovered IAM Identity Center instance
- `WebhookId` — Generated webhook ID
- `OperatorWebAppUrl` — Console URL for the Agent Space
- `SecretArn` — Secrets Manager ARN with webhook credentials

---

## Post-Deployment: Grant User Access

After deployment, users need to be granted access to the DevOps Agent Web App. This requires **two steps**:

### Step A: Assign Users to IDC Application

The IDC Application ARN can be found via:

```bash
aws devops-agent get-operator-app \
  --agent-space-id <AGENT_SPACE_ID> \
  --region us-east-1 \
  --query 'idc.idcApplicationArn' \
  --output text
```

Then assign users or groups:

```bash
# Assign a user
aws sso-admin create-application-assignment \
  --application-arn <IDC_APPLICATION_ARN> \
  --principal-id <USER_ID_FROM_IDENTITY_STORE> \
  --principal-type USER \
  --region us-east-1

# Or assign a group
aws sso-admin create-application-assignment \
  --application-arn <IDC_APPLICATION_ARN> \
  --principal-id <GROUP_ID_FROM_IDENTITY_STORE> \
  --principal-type GROUP \
  --region us-east-1
```

To find user/group IDs:

```bash
# List users
aws identitystore list-users \
  --identity-store-id <IDENTITY_STORE_ID> \
  --region us-east-1

# List groups
aws identitystore list-groups \
  --identity-store-id <IDENTITY_STORE_ID> \
  --region us-east-1
```

### Step B: Grant Access in DevOps Agent Console

1. Open the DevOps Agent console (use the `OperatorWebAppUrl` from stack outputs)
2. Navigate to the Agent Space → **Settings** → **User Access**
3. Add users/groups and assign roles (Operator, Viewer, etc.)

> **Note:** Users must sign out and back in via SSO after being granted access for the first time.

---

## Testing

### Create a Test Alarm and Trigger Investigation

```bash
# Create a test alarm
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

# Trigger ALARM state
aws cloudwatch set-alarm-state \
  --alarm-name "DevOpsAgent-Test-Alarm" \
  --state-value ALARM \
  --state-reason "Testing DevOps Agent auto-investigation" \
  --region us-east-1
```

### Verify Lambda Execution

```bash
aws logs tail "/aws/lambda/devops-webhook-executor-<STACK_NAME>" \
  --since 2m \
  --region us-east-1
```

Expected output:
```
INFO  WebHook Executor invoked
INFO  Sending webhook: { url: 'https://event-ai.us-east-1.api.aws/webhook/generic/...', payloadSize: 677 }
INFO  Response: { status: 200, body: '{"message": "Webhook received"}' }
```

### Verify Investigation in Console

Open the Agent Space in the DevOps Agent console. You should see a new investigation created with the alarm details.

---

## How It Works

### EventBridge Rule

Captures all CloudWatch Alarm state transitions to ALARM:

```json
{
  "source": ["aws.cloudwatch"],
  "detail-type": ["CloudWatch Alarm State Change"],
  "detail": {
    "state": {
      "value": ["ALARM"]
    }
  }
}
```

### Webhook Payload (DevOps Agent API Spec)

```json
{
  "eventType": "incident",
  "incidentId": "alarm-<AlarmName>-<timestamp>",
  "action": "created",
  "priority": "HIGH",
  "title": "<AlarmName>",
  "description": "CloudWatch Alarm \"<AlarmName>\" entered ALARM state.\n\nReason: ...\nAlarm ARN: ...\nRegion: us-east-1",
  "service": "<Namespace>",
  "timestamp": "<ISO8601>",
  "data": {
    "metadata": {
      "alarm_name": "...",
      "alarm_arn": "...",
      "region": "...",
      "state": "ALARM",
      "reason": "..."
    }
  }
}
```

### HMAC-SHA256 Authentication

The webhook is signed using:
- **Signature input**: `<timestamp>:<payload_json>`
- **Algorithm**: HMAC-SHA256 with the webhook secret
- **Headers**: `x-amzn-event-signature` (base64 signature) + `x-amzn-event-timestamp`

---

## Cleanup

### Delete the Stack

```bash
aws cloudformation delete-stack \
  --stack-name devops-alarm-investigation \
  --region us-east-1
```

> **Warning:** This permanently deletes the Agent Space and all investigation history.

### Clean Up Test Alarm

```bash
aws cloudwatch delete-alarms \
  --alarm-names "DevOpsAgent-Test-Alarm" \
  --region us-east-1
```

---

## Troubleshooting

| Issue | Cause | Resolution |
|-------|-------|------------|
| "You are not authorized to view this Agent Space" | User not assigned to IDC application | Run `create-application-assignment` (Step A above), then re-login via SSO |
| Lambda returns 401/403 from webhook | Webhook credentials incorrect | Check Secrets Manager secret has valid `webhookUrl` and `webhookSecret` |
| Investigation immediately cancelled | Monthly investigation limit reached | Contact AWS for rate limit increase |
| EventChannel already registered error | Previous deployment not fully cleaned up | Template handles this idempotently — the Custom Resource finds the existing service ID |
| Stack creation timeout on WebhookSetup | Lambda permissions issue | Ensure the `WebhookSetupRole` has `aidevops:*` and `sso:ListInstances` permissions |

---

## Files in This Directory

| File | Description |
|------|-------------|
| `cfn-devops-alarm-auto-investigation.yaml` | CloudFormation template (main deployment artifact) |
| `post-deploy-update-webhook.sh` | Helper script for manual webhook credential update (not needed with current template) |
| `DevOpsAgent.md` | Full DevOps Agent solution guide (Slack integration, MCP server, etc.) |
| `DevOps-Agent-Introduction.md` | Customer-facing introduction document |
| `DevOps-Agent-Introduction-CN.md` | Customer-facing introduction (Chinese) |

---

## Version History

| Date | Change |
|------|--------|
| 2026-07-22 | Initial deployment template with full automation (IDC + webhook + alarm trigger) |

---

*Author: Neo Wong (wangneo) — TAM, APJC-GCR*
