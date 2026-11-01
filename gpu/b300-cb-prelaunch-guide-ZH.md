# B300 容量块（Capacity Block）启动前准备指南（通用）

**区域（Region）：** 首尔 `ap-northeast-2`
**实例类型：** `p6-b300.48xlarge`（8 张 GPU，17 块网卡，EFA 带宽最高 6400 Gbps）
**DLAMI（首尔）：** `ami-0d8bfaf398c27480b` — x86_64、Base GPU、OSS NVIDIA 驱动、Ubuntu Pro 24.04

> 本指南介绍将实例启动进 ML 容量块（Capacity Block, CB）的通用步骤。请将所有 `<...>` 占位符替换为你自己的值。所有 CLI 示例均默认 `--region ap-northeast-2`。

---

## 前置条件 — 需要收集的输入

| 输入项 | 占位符 | 说明 |
|-------|-------|------|
| 容量块预留 id | `<cr-id>` | 例如 `cr-xxxxxxxxxxxxxxxxx` |
| 容量块可用区（AZ） | `<cb-az>` | 例如 `ap-northeast-2c`（az-id `apne2-az3`） |
| VPC id | `<vpc-id>` | 其中须有一个位于 CB 可用区的子网 |
| CB 可用区内的私有子网 id | `<subnet-id>` | 子网 AZ 必须等于 CB 的 AZ |
| EC2 密钥对名称 | `<key-name>` | 用于堡垒机 / SSH |
| 共享存储（可选） | `<fsx-dns>` / 挂载点 | 若使用，须与 CB 位于同一 AZ |

**黄金法则：** 容量块 AZ == 子网 AZ == 共享存储 AZ。三者不一致是最常见的启动失败原因。

---

## 步骤 1 — 解析 / 确认 DLAMI

### CLI
```bash
# 解析首尔区域最新的 Base GPU Ubuntu Pro 24.04 DLAMI
aws ssm get-parameter --region ap-northeast-2 \
  --name /aws/service/deeplearning/ami/x86_64/base-oss-nvidia-driver-gpu-ubuntu-pro-24.04/latest/ami-id \
  --query Parameter.Value --output text

# 查看该 AMI 的描述
aws ec2 describe-images --region ap-northeast-2 \
  --image-ids ami-0d8bfaf398c27480b \
  --query 'Images[0].[ImageId,Name,Description]' --output table
```
除非 SSM 返回了你打算采用的更新 id，否则使用 `ami-0d8bfaf398c27480b`。

### 控制台（Console）
1. EC2 → **AMIs（映像）** → 过滤器切换为 **Public images（公有映像）**。
2. 搜索 `ami-0d8bfaf398c27480b` → 确认名称/描述为 Base GPU、OSS NVIDIA 驱动、Ubuntu Pro 24.04、x86_64。
3. （可选）AWS Systems Manager → **Parameter Store（参数存储）** → 搜索上面的路径 → **Value** 即为最新 id。

---

## 步骤 2 — 创建 EFA 安全组

EFA RDMA 流量要求一个**双向、自引用、放行全部协议**的安全组。这是 EFA 配置里最常见的错误点。

### CLI
```bash
# 创建安全组
SG=$(aws ec2 create-security-group --region ap-northeast-2 \
  --group-name b300-efa --description "B300 EFA" \
  --vpc-id <vpc-id> --query GroupId --output text)

# 自引用、放行全部的入站规则
aws ec2 authorize-security-group-ingress --region ap-northeast-2 \
  --group-id $SG \
  --ip-permissions IpProtocol=-1,UserIdGroupPairs="[{GroupId=$SG}]"

# 自引用、放行全部的出站规则
aws ec2 authorize-security-group-egress --region ap-northeast-2 \
  --group-id $SG \
  --ip-permissions IpProtocol=-1,UserIdGroupPairs="[{GroupId=$SG}]"
```
可选：再从堡垒机的 SG/CIDR 增加一条入站 SSH（TCP 22）规则。

### 控制台（Console）
1. VPC（或 EC2）→ **Security groups（安全组）** → **Create security group（创建安全组）**。
2. 名称填 `b300-efa`，选择 `<vpc-id>`。
3. **入站规则** → 添加规则 → 类型 **所有流量（All traffic）**，来源 **自定义（Custom）** → 开始输入并选择**该安全组本身**（自引用）。
4. **出站规则** → 添加规则 → 类型 **所有流量**，目标 **自定义** → 选择**该安全组本身**。
5. （可选）从堡垒机 SG/CIDR 添加入站 **SSH / TCP 22** → **创建**。

---

## 步骤 3 — 创建启动模板（17 块网卡）

P6-B300 需要 **17 块网卡**才能达到满额 6400 Gbps EFA 带宽：网卡索引（NCI）0 为标准 ENA `interface`，NCI 1–16 为 `efa-only`。全部引用该 EFA 安全组和 CB 所在可用区的子网。
（参考：https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html）

### CLI
用一个小循环生成 17 网卡的启动模板数据文件：
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
注意：
- **不要**添加放置组（placement group）——容量块会拒绝带放置组的请求。
- **不要**把 CB id 写进模板——在启动时（步骤 5）再指定。
- **不要**硬编码其他 AZ——子网的 AZ 必须就是 CB 的 AZ。

### 控制台（Console）
1. EC2 → **Launch templates（启动模板）** → **Create launch template**。名称 `b300-efa`。
2. **应用和操作系统映像** → 填入 `ami-0d8bfaf398c27480b`。
3. **实例类型** → `p6-b300.48xlarge`。**密钥对** → `<key-name>`。
4. **存储** → 根卷 500 GiB，**gp3**，加密。
5. **高级详细信息** → **元数据版本** = *仅 V2（需要令牌）*。
6. **高级网络配置** → 添加网络接口：
   - 接口 1：**网卡索引 0**，设备索引 0，类型 **interface（ENA）**，安全组 `b300-efa`，子网 `<subnet-id>`。
   - 接口 2–17：**网卡索引 1 … 16**，设备索引 0，类型 **EFA-only**，相同安全组、相同子网。
7. **不要**设置放置组。点击 **Create launch template**。

---

## 步骤 4 — 预演校验（Dry-run，不消耗 CB）

### CLI
```bash
aws ec2 run-instances --region ap-northeast-2 \
  --launch-template LaunchTemplateName=b300-efa \
  --count <count> --dry-run
```
应返回 `Request would have succeeded`。若报错，在正式启动前解决。

### 控制台（Console）
启动模板 → 选择 `b300-efa` → **操作 → 从模板启动实例** → 检查表单是否有红色校验错误（不要提交）。如有标红，回去修正模板。

---

## 步骤 5 — 确认容量块就绪

### CLI
```bash
aws ec2 describe-capacity-reservations --region ap-northeast-2 \
  --capacity-reservation-ids <cr-id> \
  --query 'CapacityReservations[0].[State,AvailabilityZone,InstanceType,TotalInstanceCount,AvailableInstanceCount]' \
  --output table
```
核对：`State` 在开始时间后可用、AZ == 子网 AZ、InstanceType = `p6-b300.48xlarge`、数量相符。

### 控制台（Console）
EC2 → **Capacity Reservations（容量预留）** → 打开 `<cr-id>` → 确认状态、AZ、实例类型与数量。容量块仅在其计划开始时间之后才可用（并开始计费）。

---

## 步骤 6 — 启动进容量块（到达开始时间后）

### CLI
```bash
aws ec2 run-instances --region ap-northeast-2 \
  --launch-template LaunchTemplateName=b300-efa \
  --count <count> \
  --capacity-reservation-specification \
    'CapacityReservationTarget={CapacityReservationId=<cr-id>}'
```

### 控制台（Console）
启动模板 → `b300-efa` → **操作 → 从模板启动实例** →
**高级详细信息 → 容量预留 → 按 ID 定位（Target by ID）** → 选择 `<cr-id>` →
设置实例数量 → **启动实例**。

容量块内的实例不计入你的按需（On-Demand，G/P）vCPU 配额。

---

## 关键正确性检查清单

| # | 规则 | 原因 |
|---|------|------|
| 1 | CB AZ == 子网 AZ == 存储 AZ | 硬性要求；#1 失败原因 |
| 2 | EFA 安全组双向、自引用、放行全部 | #1 EFA 配置错误 |
| 3 | 不使用放置组 | 容量块会拒绝 |
| 4 | CB id 在启动时提供，而非写进模板 | 保持模板可复用 |
| 5 | 17 块网卡（NCI 0 = ENA，1–16 = efa-only）才能满 6400 Gbps | 正确的 B300 EFA 布局 |
| 6 | 强制 IMDSv2、加密 gp3 根卷 | 基础加固 |

---

## 来源（Sources）
- P6-B300 EFA 网卡配置 — https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html（2026-11-01 核实）
- DLAMI Base GPU（Ubuntu Pro 24.04）SSM 路径 — https://docs.aws.amazon.com/dlami/latest/devguide/aws-deep-learning-x86-base-gpu-ami-ubuntu-pro-24-04.html（2026-11-01 核实）
- 各实例类型网卡数量 — https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html

