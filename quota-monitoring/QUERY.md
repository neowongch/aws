# Query — Custom/QuotaMonitor metrics

Metrics are stored in **us-east-1**:

- Namespace: `Custom/QuotaMonitor`
- MetricName: `ResourceCount` (Unit: Count)
- Dimensions: `Service`, `QuotaName`, `AccountId`. All three must match
  exactly, including the spaces in `QuotaName`.

| Service | QuotaName |
|---|---|
| cloudfront | Distributions per AWS account |
| cloudfront | Max distributions per origin request policy |
| cloudfront | Max distributions per function |
| cloudfront | CloudFront Functions per account |
| cloudfront | Origin request policies per account |
| shield | CloudFront distribution protections |
| route53 | Hosted zones |
| route53domains | Registered domains |

Use **Statistic = Maximum** and **Period = 86400** (1 day). The Lambda
publishes one point per run. Smaller periods return mostly empty buckets.

---

## CLI

List the series:

```bash
aws cloudwatch list-metrics --region us-east-1 --namespace Custom/QuotaMonitor
```

Latest value for every quota in one call (SEARCH expression):

```bash
aws cloudwatch get-metric-data --region us-east-1 \
  --start-time $(date -u -v-14d +%Y-%m-%dT%H:%M:%SZ) \
  --end-time   $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --scan-by TimestampDescending \
  --metric-data-queries '[{"Id":"all","Expression":"SEARCH('"'"'{Custom/QuotaMonitor,Service,QuotaName,AccountId} MetricName=\"ResourceCount\"'"'"', '"'"'Maximum'"'"', 86400)"}]' \
  --query 'MetricDataResults[].[Label,Values[0],Timestamps[0]]' --output table
```

History for one quota (replace the account ID):

```bash
aws cloudwatch get-metric-statistics --region us-east-1 \
  --namespace Custom/QuotaMonitor --metric-name ResourceCount \
  --dimensions Name=Service,Value=cloudfront \
               "Name=QuotaName,Value=Distributions per AWS account" \
               Name=AccountId,Value=<ACCOUNT_ID> \
  --start-time $(date -u -v-7d +%Y-%m-%dT%H:%M:%SZ) \
  --end-time   $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 86400 --statistics Maximum
```

`date -v-14d` is the macOS form. On Linux, use `date -u -d '14 days ago' +%Y-%m-%dT%H:%M:%SZ`.

---

## Python

`query_quota_metrics.py` (in this folder) finds all series and prints the
latest value per quota:

```bash
python3 query_quota_metrics.py                 # default: last 14 days, default profile
python3 query_quota_metrics.py --days 30 --profile myprofile
```

Sample output:

```
Account       Service         Quota                                             Latest  Timestamp (UTC)
0157xxxx3084  cloudfront      CloudFront Functions per account                     4.0  2026-10-15 11:42
0157xxxx3084  cloudfront      Distributions per AWS account                        9.0  2026-10-15 11:42
0157xxxx3084  route53         Hosted zones                                         2.0  2026-10-15 11:42
0157xxxx3084  shield          CloudFront distribution protections                  7.0  2026-10-15 11:42
...
```

Required IAM: `cloudwatch:ListMetrics`, `cloudwatch:GetMetricData`
(and `cloudwatch:GetMetricStatistics` for the single-quota CLI command).

---

## Notes

- Retention: daily-period custom metrics are kept for 15 months.
- The stack publishes **counts only**. To get utilization, divide by the
  quota limit, e.g.
  `aws service-quotas get-service-quota --region us-east-1 --service-code cloudfront --quota-code <code>`.
