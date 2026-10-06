# 边缘数据采集与协议转换设计说明

本文档说明本地边缘网关的采集协议、统一数据模型、缓冲与去重策略、版本兼容规则和验证方法。
整套实现只依赖 Go 标准库，设备与下游均为本地模拟，无需外部服务、真实硬件或联网。

## 1. 总体链路

```
模拟设备 RawFrame
   -> 协议解码 (protocol: modbus / opcua)
   -> 版本语义转换 (transform: 按 协议@版本 查规则表)
   -> 时间戳/量程校验 (gateway)
   -> 幂等去重 (deviceID + seq)
   -> 本地持久化缓冲 (spool: WAL)
   -> 下游投递 (sink)，失败留本地按 FIFO 重试
```

异常数据绝不静默丢弃：任何被拒绝的帧都生成一条 `model.RejectRecord`，
携带**可区分的原因码**、可读明细以及**完整原始报文**（`RawFrame.Raw`），
可通过 `Gateway.Rejects()` 审计追溯。

## 2. 统一数据模型

归一结果 `model.Reading`：

| 字段 | 含义 |
| --- | --- |
| `device_id` | 设备标识 |
| `metric` | 标准测点名（`temperature` / `pressure`） |
| `value` | 归一后的浮点值 |
| `unit` | 标准单位（温度 `C`，压力 `hPa`） |
| `timestamp` | UTC 时间 |
| `frame_id` | 幂等键 `deviceID:seq`，可追溯回原始报文 |
| `protocol` / `version` | 来源协议与版本，便于审计 |

### 拒绝原因码（`model.RejectReason`）

| 原因码 | 触发场景 |
| --- | --- |
| `UNKNOWN_PROTOCOL` | 上报协议未在协议注册表登记 |
| `UNSUPPORTED_VERSION` | 协议已知，但该版本没有转换规则（不猜测） |
| `MALFORMED_FRAME` | 报文不是合法 JSON 等结构性错误 |
| `MISSING_FIELD` | `device_id`、必填测点、OPC-UA 质量戳缺失/非 GOOD |
| `FIELD_TYPE` | 字段类型与声明不符（如寄存器值是字符串、RFC3339 时间戳给数字） |
| `TIMESTAMP_OUT_OF_RANGE` | 时间戳缺失、无法解析，或相对网关时钟超出 `±窗口` |
| `VALUE_OUT_OF_RANGE` | 归一后数值是 NaN/Inf 或超出该测点允许量程 |
| `DUPLICATE_FRAME` | 同设备同序号报文重复上报（统计计数，日志可见） |
| `BUFFER_OVERFLOW` | 缓冲达上限时的背压错误或显式丢弃审计 |

时间戳窗口可配置，默认未来 5 分钟、过去 24 小时。

## 3. 采集协议（本地模拟）

### 3.1 Modbus（JSON 化的寄存器映射）

```json
{"device_id":"plc-line1","ts":1760000000,"regs":{"40001":251,"40002":1013}}
```

### 3.2 OPC-UA（JSON 化的节点订阅）

```json
{"device_id":"opc-main","ts":"2026-10-07T04:00:00Z",
 "nodes":{"temperature":{"value":312,"quality":"GOOD"},
          "pressure":{"value":101300,"quality":"GOOD"}}}
```

解码严格使用 `json.Decoder.UseNumber()`：数字、字符串、布尔不做隐式互转，
因此“字段类型异常”能被确定性识别（例如 `40001` 给 `"hot"` 直接 `FIELD_TYPE`）。

## 4. 版本兼容与语义转换规则

规则以 `协议@版本` 为键登记在 `transform.RuleBook`，每个测点规则包含
`归一值 = 原始值 * Scale + Offset`、标准单位与允许量程。

| 协议版本 | 温度原始量 | 归一 | 压力原始量 | 归一 | 时间戳 |
| --- | --- | --- | --- | --- | --- |
| `modbus@1` | 0.1 °C 缩放整数 (`251`) | `25.1 C` | kPa (`1013`) | `1013 hPa` | Unix 秒 |
| `modbus@2` | °C 原值 (`25.1`) | `25.1 C` | MPa (`0.1013`) | `1013 hPa` (×10000) | Unix 毫秒 |
| `opcua@1` | 0.1 °C (`312`) | `31.2 C` | Pa (`101300`) | `1013 hPa` (×0.01) | RFC3339 字符串 |

量程：温度 `[-40, 125] C`，压力 `[800, 1200] hPa`。

- 已知版本按规则精确归一，单位、缩放、偏移、时间戳精度差异全部消除。
- **未知版本或规则缺失**：返回 `UNSUPPORTED_VERSION`，绝不猜测默认规则。
- 新增协议版本 = 在 `transform.DefaultRules()` 增加一条规则，无需改动管线。

## 5. 缓冲、去重与恢复

### 5.1 幂等键

`frame_id = deviceID + ":" + seq`。同一设备重连后重放旧报文、进程重启后设备重传、
或转发过程中的重复推送，只要设备 ID 与序号相同就判定为同一帧：

- 已在队列：不重复入队；
- 已成功投递：直接忽略并计入 `Duplicate`；
- 下游 `LocalSink` 也按帧幂等，即使 “Send 成功但 ACK 落盘失败” 导致重发，也不会重复入库。

### 5.2 持久化（WAL）

`spool.Spool` 在 `spool.wal` 中追加三类记录：`enqueue` / `deliver` / `evict`，
每条记录 `fsync` 落盘。进程重启时重放日志即可还原
“待发送 FIFO 队列 + 近期已投递指纹集合”，因此：

- 下游故障期间崩溃重启，待发数据不丢；
- 已确认帧重启后不重发，设备重传也会被判重；
- 日志在已投递指纹积累过多时做原子压缩（保留最近 128 个指纹），避免无限增长。

### 5.3 容量上限策略（显式、可解释，无静默丢弃）

| 策略 | 满时行为 |
| --- | --- |
| `PolicyBlock`（背压） | `Enqueue` 返回 `spool.ErrOverflow`；`HandleFrame` 将其作为 Go error 交给上游，上游稍后重试同一帧，数据不丢 |
| `PolicyDropOldest` | 丢弃 FIFO 队首最旧帧，写入 `evict` 日志并在 `Spool.Evictions()` 暴露审计记录（含帧 ID、读数、原因、容量说明），再放入新帧 |

容量按“帧”计（一帧含多个测点读数，同帧原子入队）。

### 5.4 转发与补传

转发器单协程消费共享 spool，永远从最旧帧开始；任一帧投递失败即暂停本轮，
按固定间隔重试，保证 FIFO 顺序与多路设备数据不错乱。

## 6. 并发模型

多路设备的 `HandleFrame` 可被并发调用：判重、入队、统计都在锁内完成，
spool 自身互斥。`TestConcurrentDevicesSharedSpool` 以 8 设备 × 25 帧并发、
在 `-race` 下校验“每帧恰好 2 条读数、顺序归属正确、不丢不重”。

## 7. 验证方法

全部本地可复现（Go 1.25+，竞态检测需要 cgo/gcc）：

```bash
make test-race     # 竞态检测 + 详细用例日志
make test          # 普通单测
make demo          # 演示：断连/补传/重复/版本与字段异常/DropOldest
make demo-block    # 演示：Block 背压策略
```

### 测试与业务场景对照

| 测试用例 | 覆盖业务 |
| --- | --- |
| `TestDisconnectReconnectAndReplay` | 设备断连不产数、重连重放、重复上报幂等 |
| `TestSpoolReplayAfterOutage` | 下游不可用落本地、恢复后 FIFO 补传、ACK 后重报不重 |
| `TestRestartNoLossNoDuplicate` | 进程重启 WAL 恢复，不丢不重 |
| `TestDuplicateWhilePending` | 未投递阶段重复上报不重复入队 |
| `TestVersionSemanticsAndUnknown` | v1/v2 单位/缩放/时间戳归一，未知版本拒绝 |
| `TestRejectReasons`（9 个子用例） | 字段类型、时间戳越界、必填缺失、量程越界、畸形报文、未知协议等原因码可区分且保留原始报文 |
| `TestOverflowPolicies` | Block 背压可重试；DropOldest 显式丢弃有审计 |
| `TestTransientSinkFailure` | 下游瞬时故障自动重试、不丢不重 |
| `TestConcurrentDevicesSharedSpool` | 多路并发共享缓冲，`-race` 下不错乱 |

测试日志中每个业务动作带 `[business]` 标签（入缓冲/投递成功/下游不可用/
重复上报/拒绝/背压/显式丢弃），用例名直接对应业务场景；
`go test -v` 可看到每个用例及其子用例的通过情况。
