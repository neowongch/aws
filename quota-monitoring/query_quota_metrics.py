"""Query Custom/QuotaMonitor ResourceCount metrics (latest value per quota).

Usage:
    python3 query_quota_metrics.py [--days 14] [--profile NAME] [--namespace Custom/QuotaMonitor]
"""
import argparse
from datetime import datetime, timedelta, timezone

import boto3

ap = argparse.ArgumentParser()
ap.add_argument("--days", type=int, default=14, help="Look-back window in days")
ap.add_argument("--profile", default=None, help="AWS CLI profile")
ap.add_argument("--namespace", default="Custom/QuotaMonitor")
args = ap.parse_args()

NS = args.namespace
cw = boto3.Session(profile_name=args.profile).client("cloudwatch", region_name="us-east-1")

# 1. Discover every (Service, QuotaName, AccountId) series
metrics = []
for page in cw.get_paginator("list_metrics").paginate(Namespace=NS, MetricName="ResourceCount"):
    metrics.extend(page["Metrics"])
if not metrics:
    raise SystemExit(f"No metrics found in {NS} (us-east-1). Has the Lambda run yet?")

# 2. One query per series (GetMetricData allows up to 500 per call)
end = datetime.now(timezone.utc)
start = end - timedelta(days=args.days)
queries, labels = [], {}
for i, m in enumerate(metrics):
    qid = f"m{i}"
    labels[qid] = {d["Name"]: d["Value"] for d in m["Dimensions"]}
    queries.append({
        "Id": qid,
        "MetricStat": {
            "Metric": {"Namespace": NS, "MetricName": "ResourceCount", "Dimensions": m["Dimensions"]},
            "Period": 86400,  # daily buckets; the Lambda publishes once a day
            "Stat": "Maximum",
        },
    })

results = []
for page in cw.get_paginator("get_metric_data").paginate(
    MetricDataQueries=queries, StartTime=start, EndTime=end, ScanBy="TimestampDescending"
):
    results.extend(page["MetricDataResults"])

# 3. Print the newest datapoint per quota
print(f"{'Account':<14}{'Service':<16}{'Quota':<48}{'Latest':>8}  Timestamp (UTC)")
for r in sorted(results, key=lambda r: (labels[r["Id"]]["Service"], labels[r["Id"]]["QuotaName"])):
    d = labels[r["Id"]]
    if r["Values"]:
        val, ts = r["Values"][0], r["Timestamps"][0].strftime("%Y-%m-%d %H:%M")
    else:
        val, ts = "-", "no data"
    print(f"{d['AccountId']:<14}{d['Service']:<16}{d['QuotaName']:<48}{val!s:>8}  {ts}")
