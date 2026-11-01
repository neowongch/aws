# B300 Capacity Block — Pre-Launch Preparation Guide (Generic)

**Region:** Seoul `ap-northeast-2`
**Instance type:** `p6-b300.48xlarge` (8 GPUs, 17 network cards, up to 6400 Gbps EFA)
**DLAMI (Seoul):** `ami-0d8bfaf398c27480b` — x86_64, Base GPU, OSS NVIDIA Driver, Ubuntu Pro 24.04

> This guide covers the common steps to prepare for and launch instances into an EC2 Capacity Block (CB) for ML. Replace every `<...>` placeholder with your own values. All CLI examples assume `--region ap-northeast-2`.

---

## Prerequisites — inputs to gather

| Input | Placeholder | Note |
|-------|-------------|------|
| Capacity Block reservation id | `<cr-id>` | e.g. `cr-xxxxxxxxxxxxxxxxx` |
| CB Availability Zone | `<cb-az>` | e.g. `ap-northeast-2c` (az-id `apne2-az3`) |
| VPC id | `<vpc-id>` | Must contain a subnet in the CB AZ |
| Private subnet id in the CB AZ | `<subnet-id>` | Subnet AZ MUST equal the CB AZ |
| EC2 key pair name | `<key-name>` | For bastion / SSH |
| Shared storage (optional) | `<fsx-dns>` / mount | If used, place it in the CB AZ |

**Golden rule:** Capacity Block AZ == subnet AZ == shared-storage AZ. A mismatch is the most common launch failure.

---

## Step 1 — Resolve / confirm the DLAMI

### CLI
```bash
# Resolve latest Base GPU Ubuntu Pro 24.04 DLAMI in Seoul
aws ssm get-parameter --region ap-northeast-2 \
  --name /aws/service/deeplearning/ami/x86_64/base-oss-nvidia-driver-gpu-ubuntu-pro-24.04/latest/ami-id \
  --query Parameter.Value --output text

# Inspect the AMI description
aws ec2 describe-images --region ap-northeast-2 \
  --image-ids ami-0d8bfaf398c27480b \
  --query 'Images[0].[ImageId,Name,Description]' --output table
```
Use `ami-0d8bfaf398c27480b` unless SSM returns a newer id you intend to adopt.

### Console
1. EC2 → **AMIs** → switch filter to **Public images**.
2. Search `ami-0d8bfaf398c27480b` → confirm Name/Description = Base GPU, OSS NVIDIA Driver, Ubuntu Pro 24.04, x86_64.
3. (Optional) AWS Systems Manager → **Parameter Store** → search the path above → **Value** is the latest id.

---

## Step 2 — Create the EFA security group

A self-referencing allow-all security group (both directions) is required for EFA RDMA traffic. This is the single most common EFA mistake.

### CLI
```bash
# Create SG
SG=$(aws ec2 create-security-group --region ap-northeast-2 \
  --group-name b300-efa --description "B300 EFA" \
  --vpc-id <vpc-id> --query GroupId --output text)

# Self-referencing allow-all INGRESS
aws ec2 authorize-security-group-ingress --region ap-northeast-2 \
  --group-id $SG \
  --ip-permissions IpProtocol=-1,UserIdGroupPairs="[{GroupId=$SG}]"

# Self-referencing allow-all EGRESS
aws ec2 authorize-security-group-egress --region ap-northeast-2 \
  --group-id $SG \
  --ip-permissions IpProtocol=-1,UserIdGroupPairs="[{GroupId=$SG}]"
```
Optionally add an inbound SSH (TCP 22) rule from your bastion's SG/CIDR.

### Console
1. VPC (or EC2) → **Security groups** → **Create security group**.
2. Name `b300-efa`, select `<vpc-id>`.
3. **Inbound rules** → Add rule → Type **All traffic**, Source **Custom** → start typing and select **this same security group** (self-reference).
4. **Outbound rules** → Add rule → Type **All traffic**, Destination **Custom** → select **this same security group**.
5. (Optional) add Inbound **SSH / TCP 22** from the bastion SG/CIDR → **Create**.

---

## Step 3 — Create the launch template (17 network interfaces)

P6-B300 reaches full 6400 Gbps EFA bandwidth with **17 network cards**: NCI 0 is a standard ENA `interface`, NCI 1–16 are `efa-only`. All reference the EFA SG and the CB-AZ subnet.
(Ref: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html)

### CLI
Generate the launch-template data file (17 NICs) with a small loop:
```bash
SG=<efa-sg-id>; SUBNET=<subnet-id>; KEY=<key-name>; AMI=ami-0d8bfaf398c27480b

nics='[{"NetworkCardIndex":0,"DeviceIndex":0,"InterfaceType":"interface","Groups":["'$SG'"],"SubnetId":"'$SUBNET'","DeleteOnTermination":true}'
for i in $(seq 1 16); do
  nics=$nics',{"NetworkCardIndex":'$i',"DeviceIndex":0,"InterfaceType":"efa-only","Groups":["'$SG'"],"SubnetId":"'$SUBNET'","DeleteOnTermination":true}'
done
nics=$nics']'

cat > b300-lt-data.json <<JSON
{
  "ImageId": "$AMI",
  "InstanceType": "p6-b300.48xlarge",
  "KeyName": "$KEY",
  "BlockDeviceMappings": [
    {"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":500,"VolumeType":"gp3","Encrypted":true,"DeleteOnTermination":true}}
  ],
  "MetadataOptions": {"HttpTokens":"required","HttpEndpoint":"enabled"},
  "NetworkInterfaces": $nics
}
JSON

aws ec2 create-launch-template --region ap-northeast-2 \
  --launch-template-name b300-efa \
  --launch-template-data file://b300-lt-data.json
```
Notes:
- Do **not** add a placement group (a Capacity Block rejects it).
- Do **not** put the CB id in the template — specify it at launch (Step 5).
- Do **not** hardcode a different AZ — the subnet's AZ must be the CB AZ.

### Console
1. EC2 → **Launch templates** → **Create launch template**. Name `b300-efa`.
2. **Application and OS Image** → enter `ami-0d8bfaf398c27480b`.
3. **Instance type** → `p6-b300.48xlarge`. **Key pair** → `<key-name>`.
4. **Storage** → root volume 500 GiB, **gp3**, Encrypted.
5. **Advanced details** → **Metadata version** = *V2 only (token required)*.
6. **Advanced network configuration** → add interfaces:
   - Interface 1: **Network card index 0**, Device index 0, type **interface (ENA)**, SG `b300-efa`, subnet `<subnet-id>`.
   - Interfaces 2–17: **Network card index 1 … 16**, Device index 0, type **EFA-only**, same SG, same subnet.
7. Do **not** set a placement group. **Create launch template**.

---

## Step 4 — Dry-run validate (no CB consumed)

### CLI
```bash
aws ec2 run-instances --region ap-northeast-2 \
  --launch-template LaunchTemplateName=b300-efa \
  --count <count> --dry-run
```
Expect `Request would have succeeded`. Resolve any error now, before go-live.

### Console
Launch templates → select `b300-efa` → **Actions → Launch instance from template** → review the form for red validation errors (do not submit). Fix the template if anything is flagged.

---

## Step 5 — Confirm Capacity Block readiness

### CLI
```bash
aws ec2 describe-capacity-reservations --region ap-northeast-2 \
  --capacity-reservation-ids <cr-id> \
  --query 'CapacityReservations[0].[State,AvailabilityZone,InstanceType,TotalInstanceCount,AvailableInstanceCount]' \
  --output table
```
Verify: `State` is usable at the start time, AZ == subnet AZ, InstanceType = `p6-b300.48xlarge`, count matches.

### Console
EC2 → **Capacity Reservations** → open `<cr-id>` → confirm State, AZ, instance type and count. A Capacity Block becomes usable (and billable) only from its scheduled start time.

---

## Step 6 — Launch into the Capacity Block (at start time)

### CLI
```bash
aws ec2 run-instances --region ap-northeast-2 \
  --launch-template LaunchTemplateName=b300-efa \
  --count <count> \
  --capacity-reservation-specification \
    'CapacityReservationTarget={CapacityReservationId=<cr-id>}'
```

### Console
Launch templates → `b300-efa` → **Actions → Launch instance from template** →
**Advanced details → Capacity reservation → Target by ID** → select `<cr-id>` →
set the instance count → **Launch instance**.

Capacity Block instances do not count against your On-Demand (G/P) vCPU limits.

---

## Key correctness checklist

| # | Rule | Why |
|---|------|-----|
| 1 | CB AZ == subnet AZ == storage AZ | Hard requirement; #1 failure cause |
| 2 | EFA SG self-referencing allow-all, both directions | #1 EFA misconfiguration |
| 3 | No placement group | Capacity Block rejects it |
| 4 | CB id supplied at launch, not in template | Keeps the template reusable |
| 5 | 17 NICs (NCI 0 = ENA, 1–16 = efa-only) for full 6400 Gbps | Correct B300 EFA layout |
| 6 | IMDSv2 required, encrypted gp3 root | Baseline hardening |

---

## Sources
- P6-B300 EFA network-card configuration — https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html (verified 2026-11-01)
- DLAMI Base GPU (Ubuntu Pro 24.04) SSM path — https://docs.aws.amazon.com/dlami/latest/devguide/aws-deep-learning-x86-base-gpu-ami-ubuntu-pro-24-04.html (verified 2026-11-01)
- Network cards per instance type — https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html
