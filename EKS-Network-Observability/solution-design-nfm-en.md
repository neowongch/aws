# EKS Network Observability — Solution Design

## Problem Statement

The EKS production cluster (`DBA-EKS-Prod`, us-east-1) experiences intermittent TCP connection latency between application pods (node group `heavy-load-133`, m7i.8xlarge) and the OceanBase database accessed via cross-account VPC PrivateLink endpoint.

Current troubleshooting is limited to:
- Customer-side slow logs (application layer only)
- AWS Dexter/Pimms investigation (requires case escalation, post-hoc)

There is no continuous, pod-level network performance baseline to distinguish between AWS infrastructure issues and application/third-party (OceanBase provider) issues.

## Proposed Solution

Deploy **Amazon CloudWatch Network Flow Monitor (NFM)** with the **EKS Container Network Observability** feature to provide continuous, pod-level TCP performance monitoring.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  EKS Cluster (DBA-EKS-Prod, us-east-1)                         │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │  App Pod    │  │  App Pod    │  │  App Pod    │            │
│  │  (OB client)│  │  (OB client)│  │  (OB client)│            │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘            │
│         │                 │                 │                    │
│  ┌──────┴─────────────────┴─────────────────┴──────┐           │
│  │           Worker Node (m7i.8xlarge)              │           │
│  │  ┌──────────────────────────────────────────┐   │           │
│  │  │  NFM Agent (DaemonSet, eBPF-based)       │   │           │
│  │  │  - Captures TCP flow metrics per pod     │   │           │
│  │  │  - Reports every 30s to NFM backend      │   │           │
│  │  │  - Exposes OpenMetrics for Prometheus     │   │           │
│  │  └──────────────────────────────────────────┘   │           │
│  └─────────────────────────────────────────────────┘           │
│                                                                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │ TCP flows via VPC Endpoint
                           ▼
              ┌────────────────────────┐
              │  OceanBase (cross-acct  │
              │  PrivateLink service)   │
              └────────────────────────┘
```

## What You Get

### Flow-Level Metrics (per source pod to destination)

| Metric | Description | Use Case |
|--------|-------------|----------|
| TCP Retransmissions (RT) | Packets re-sent due to loss or corruption | Detect packet loss between pod and VPCE |
| TCP Retransmission Timeouts (RTO) | Sender timer expired without ACK | Detect sustained connectivity issues (more severe than RT) |
| Data Transferred (bytes) | Volume per flow | Baseline normal traffic; detect anomalies |
| Round-Trip Time (RTT) | TCP handshake latency | Measure network path latency |

### System Metrics (per pod/node, Prometheus-scrapable)

| Metric | Description | Use Case |
|--------|-------------|----------|
| `ingress_flow` / `egress_flow` | TCP connection counts | Track connection churn |
| `ingress_bytes` / `egress_bytes` | Byte counters | Bandwidth utilization |
| `bw_in_allowance_exceeded` | Inbound bandwidth limit hit | **Direct evidence of host-level contention** |
| `bw_out_allowance_exceeded` | Outbound bandwidth limit hit | **Direct evidence of host-level contention** |
| `pps_allowance_exceeded` | Packet-per-second limit hit | Network throttling detection |
| `conntrack_allowance_exceeded` | Connection tracking limit hit | Connection table exhaustion |

### Network Health Indicator (NHI)

- Binary indicator per monitor: was there an **AWS-caused** network impairment?
- Eliminates guesswork: if NHI = Degraded, the issue is on AWS side; if NHI = Healthy, look at application/provider side

### Console Visualizations (EKS Console > Observability > Network)

- **Service Map**: Visual graph of pod-to-pod communication with RT/RTO/DT overlays
- **Flow Table**: Top talkers ranked by retransmissions — filterable by:
  - Cluster view (intra-AZ, inter-AZ)
  - AWS service view (pods to S3/DynamoDB)
  - External view (pods to VPCE/internet)

## Implementation Steps

### Step 1: Create NFM Scope (one-time per account)

```bash
ACCOUNT_ID="231038569223"
REGION="us-east-1"

aws networkflowmonitor create-scope \
  --targets "[{\"targetIdentifier\":{\"targetId\":{\"accountId\":\"${ACCOUNT_ID}\"},\"targetType\":\"ACCOUNT\"},\"region\":\"${REGION}\"}]" \
  --region $REGION
```

### Step 2: Install NFM Agent EKS Add-on

```bash
aws eks create-addon \
  --cluster-name DBA-EKS-Prod \
  --addon-name aws-network-flow-monitoring-agent \
  --region us-east-1
```

IAM: Attach `CloudWatchNetworkFlowMonitorAgentPublishPolicy` via Pod Identity or IRSA.

### Step 3: Create NFM Monitor with EKS Cluster as Local Resource

```bash
CLUSTER_ARN="arn:aws:eks:us-east-1:231038569223:cluster/DBA-EKS-Prod"
SCOPE_ARN="<from step 1>"

aws networkflowmonitor create-monitor \
  --monitor-name "dba-eks-prod-monitor" \
  --local-resources "type=AWS::EKS::Cluster,identifier=${CLUSTER_ARN}" \
  --remote-resources "type=AWS::Region,identifier=us-east-1" \
  --scope-arn "$SCOPE_ARN" \
  --region us-east-1
```

### Step 4 (Optional): Enable Prometheus Scraping

Override add-on configuration to expose OpenMetrics endpoint:

```yaml
OPEN_METRICS: "on"
OPEN_METRICS_ADDRESS: "0.0.0.0"
OPEN_METRICS_PORT: "9090"
```

Then configure your Prometheus stack to scrape the NFM agent pods on port 9090.

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Local resource = EKS cluster (not subnet) | Enables pod-level granularity with namespace/node metadata |
| Remote resource = Region | Captures all traffic including to VPCE endpoints |
| NFM agent via EKS add-on (not standalone) | DaemonSet with pod metadata enrichment |
| OpenMetrics enabled | Integrates with existing Prometheus/Grafana stack |

## How This Solves the Current Problem

| Current State | With NFM |
|---------------|----------|
| Customer reports "latency jitter" from app logs | Continuous RT/RTO metrics per pod, per destination |
| Must open support case for infrastructure investigation | Self-service: check NHI for AWS attribution |
| Cannot distinguish AWS infra vs OceanBase provider issue | Flow metrics show exactly where retransmissions occur |
| "Noisy neighbor" claim without evidence | `bw_*_allowance_exceeded` metrics prove/disprove host contention |
| Post-hoc investigation only | Real-time alerting on RT/RTO spikes |

## Prerequisites

- EKS cluster version: any supported version
- NFM agent add-on minimum: v1.1.0
- Pods must NOT use `hostNetwork: true` (pods need their own network namespace)
- IAM: `CloudWatchNetworkFlowMonitorAgentPublishPolicy` for NFM agent pods

## Limitations

- Top Contributors API aggregates over 1-hour windows (not real-time per-second)
- Agent collects top 500 flows by data volume every 30 seconds
- RTT data can be sparse (not calculated for every flow)
- NFM supported regions: check availability (us-east-1 is supported)
- Scale limit: ~5,000 worker nodes per account with NFM agent

## Cost

- Network Flow Monitor pricing: based on number of metrics published
- No additional EC2/network charges (eBPF-based, negligible CPU/memory overhead)
- See: https://aws.amazon.com/cloudwatch/pricing/ (Network Flow Monitor section)

## Next Steps

1. Deploy NFM on `DBA-EKS-Prod` cluster (account 231038569223)
2. Establish 1-week baseline of RT/RTO for heavy-load-133 -> OceanBase VPCE flows
3. Set CloudWatch Alarm on RT spike (threshold TBD based on baseline)
4. On next occurrence of "jitter", check:
   - NHI value (AWS attribution)
   - `bw_*_allowance_exceeded` (host contention evidence)
   - RT/RTO for specific pod -> VPCE flow (pinpoint affected pods)
5. Use evidence to drive resolution: dedicated instances (if allowance exceeded), OceanBase provider investigation (if NHI healthy + no allowance exceeded), or AWS case with concrete data



ref:

![alt text](Capture-2026-08-21_13-35-47.jpg)

![alt text](Capture-2026-08-21_13-36-45.jpg)

![alt text](Capture-2026-08-21_16-12-02.jpg)

![alt text](Capture-2026-08-21_16-11-50.jpg)

![alt text](Capture-2026-08-21_16-11-43.jpg)
