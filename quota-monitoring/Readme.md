# Deploy — quota-monitor-inline.yaml

Single self-contained template (Lambda code is inline — no S3, no zip).

> **Region: deploy in `us-east-1`.** The function talks to CloudFront, Shield,
> Route 53, and Route 53 Domains (all global/us-east-1 services) and publishes
> metrics to CloudWatch in `us-east-1`. Deploying elsewhere will not move the
> metrics — they always land in `us-east-1`.

The stack creates: 1 Lambda (`index.handler`), its IAM role, a log group,
a daily EventBridge Scheduler schedule + its invoke role.

Parameters (all have defaults — override only if needed):

| Parameter | Default | Notes |
|---|---|---|
| `MetricNamespace` | `Custom/QuotaMonitor` | Metric namespace; IAM is scoped to this exact value. |
| `ScheduleExpression` | `rate(1 day)` | Run cadence. e.g. `rate(6 hours)` or a `cron(...)`. |
| `LambdaRuntime` | `python3.13` | Or `python3.12`. |
| `LogRetentionDays` | `30` | CloudWatch Logs retention. |

---

## Method 1 — AWS CLI

Run from the folder containing `quota-monitor-inline.yaml`:

```bash
aws cloudformation deploy \
  --region us-east-1 \
  --stack-name quota-monitor \
  --template-file quota-monitor-inline.yaml \
  --capabilities CAPABILITY_IAM
```

Override a parameter (optional):

```bash
aws cloudformation deploy \
  --region us-east-1 \
  --stack-name quota-monitor \
  --template-file quota-monitor-inline.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ScheduleExpression='rate(6 hours)'
```

`--capabilities CAPABILITY_IAM` is required because the stack creates IAM roles.

---

## Method 2 — AWS Console

1. Switch the console region to **N. Virginia (us-east-1)** (top-right selector).
2. Go to **CloudFormation** → **Create stack** → **With new resources (standard)**.
3. **Prepare template** → *Template is ready* → **Upload a template file** →
   **Choose file** → select `quota-monitor-inline.yaml` → **Next**.
4. **Stack name**: `quota-monitor`. Leave parameters at defaults (or edit
   `ScheduleExpression` etc.) → **Next**.
5. **Configure stack options**: nothing required → **Next**.
6. On the review page, tick **"I acknowledge that AWS CloudFormation might
   create IAM resources."** → **Submit**.
7. Wait for status **CREATE_COMPLETE**. The **Outputs** tab shows the function
   name, ARN, metric namespace, and schedule name.

---

## Verify it works

**Run it once now (don't wait for the daily schedule):**

CLI —
```bash
aws lambda invoke --region us-east-1 \
  --function-name <FunctionName-from-Outputs> /dev/stdout
```
Console — Lambda → the function → **Test** → create an empty `{}` event → **Test**.

**Confirm metrics (in us-east-1):**
```bash
aws cloudwatch list-metrics --region us-east-1 --namespace Custom/QuotaMonitor
```
Or Console → **CloudWatch (us-east-1)** → **All metrics** → **Custom namespaces**
→ **Custom/QuotaMonitor** → `Service, QuotaName, AccountId`. Set **Statistic =
Maximum**, **Period = 1 hour** and widen the time range (single daily points can
look empty on a line graph).

**Logs:** CloudWatch → Log groups → `/aws/lambda/<FunctionName>`.

---

## Update / remove

- **Change code or a parameter:** edit the YAML (or pass new
  `--parameter-overrides`) and re-run the same `deploy` command / re-upload in
  the console. The stack updates in place.
- **Delete everything:** `aws cloudformation delete-stack --region us-east-1
  --stack-name quota-monitor` (or Console → select stack → **Delete**).

> Inline code is capped at **4,096 characters**. Current code is ~2,490. If you
> grow it past the cap, switch to `quota-monitor.yaml` (S3-based).
