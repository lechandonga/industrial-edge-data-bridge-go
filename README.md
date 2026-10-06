# industrial-edge-data-bridge-go

本地边缘网关的数据采集与协议转换实现。设备与下游接收端均为本地模拟
(`internal/sim`)，不依赖外部服务、真实硬件或联网依赖，全部场景可本地复现。

## 架构

```
模拟设备(sim.Device) --原始报文--> 协议解析与归一(protocol.Registry)
   --> 持久化去重缓冲(buffer.Buffer) --> 转发(gateway.Gateway.FlushPending)
   --> 模拟下游(sim.Sink)
```

- `internal/model`: 统一数据模型 `Batch`/`Point`，可区分的拒绝原因 `RejectError`。
- `internal/protocol`: 按 (协议, 版本) 注册的显式转换规则，解析 + 校验 + 归一。
- `internal/buffer`: WAL 文件持久化缓冲，去重、容量上限、背压/丢弃策略。
- `internal/gateway`: 采集入口 `IngestRaw` 与补传 `FlushPending`。
- `internal/sim`: 本地模拟设备与下游接收端。

## 采集协议

设备报文为 JSON：`{"batch_id","device_id","version","ts"(unix毫秒),"fields":{...}}`。
内置两个模拟协议族（`protocol.DefaultRegistry`）：

| 协议 | 版本 | 字段语义 |
| --- | --- | --- |
| `modsim` | `v1` | `temp` = 原始值 × 0.1 (°C)；`pressure` = 原始值 × 0.01 (kPa) |
| `modsim` | `v2` | `temp` 为华氏度 → 摄氏度；`pressure` 为 psi → kPa |
| `jsonline` | `v1` | `temp` 已是 °C；`humidity` 为百分比 |

归一化后统一为 `model.Point{DeviceID, Metric, Value, Unit, Timestamp}`，
`Batch` 保留原始报文 `Raw` 与 `BatchID`，任何拒绝/异常都可追溯回原始报文。

## 拒绝策略（不静默丢弃）

所有异常记录返回 `*model.RejectError`，`Reason` 可区分：

- `missing_field` 必填字段缺失（batch_id / device_id / 规则要求的字段）
- `bad_field_type` 字段类型异常（如温度是字符串）
- `timestamp_out_of_range` 时间戳早于 2000-01-01 或超过当前时间 24h
- `unknown_version` 未注册的协议版本
- `missing_rule` 缺少转换规则
- `value_out_of_range` 归一化后超出量程

拒绝记录会被网关记录（`Gateway.Rejects()`）并打日志，携带 `BatchID` 可追溯。

## 缓冲与去重策略

- **持久化**: 每次 `Put`/`Ack` 先追加写 WAL（`buffer.wal`，写后 fsync）再更新内存，
  进程重启后重放 WAL 恢复：未确认（未 Ack）的批次继续待补传，已确认的批次
  留在去重集合中，不会重发。
- **去重**: 以设备报文的 `batch_id` 为全局唯一键。重复上报（包括重启后的重复）
  识别为 duplicate，只保留一份，下游恰好收到一次。
- **补传**: 下游不可用期间数据落盘缓冲；`FlushPending` 按 FIFO 顺序发送，
  遇下游失败即停，未 Ack 的数据留待下次（或重启后）重试，保证不丢不重。
- **容量上限**: 达到容量时按构造时选择的策略处理，绝不静默丢数据：
  - `PolicyBackpressure`: `Put` 返回 `ErrBufferFull`，由上游施加背压；
  - `PolicyDropOldest`: 驱逐最旧未确认批次，驱逐数量通过 `PutResult.DroppedOldest`
    显式返回并计日志（`BUFFER_FULL drop-oldest evicted=N`）。

## 版本兼容规则

- 转换规则按 `(协议, 版本)` 显式注册，包含缩放系数、偏移（单位换算）、
  目标单位与量程。
- 已知版本严格按规则归一（如 v2 华氏度 → 摄氏度）。
- 未知版本或规则缺失直接拒绝（`unknown_version` / `missing_rule`），绝不猜测。
- 多路设备并发采集共享同一缓冲，缓冲内部互斥保护，`-race` 下验证无错乱。

## 验证方法

```bash
# 竞态检测需要 cgo
CGO_ENABLED=1 go test -race -v ./...
```

`-v` 日志中每个用例都打印覆盖的业务场景与结果，包括：

- 断连重连与缓冲回放（`TestDownstreamOutageAndReplay`、
  `TestRestartMidOutageKeepsData`、`TestRestartRecovery`）
- 重复上报去重（`TestDuplicateReportNotDeliveredTwice`、`TestDuplicateReportDedup`）
- 版本不兼容拒绝（`TestIncompatibleVersionRejected`、`TestRejectUnknownVersion`）
- 字段类型/必填缺失/时间戳越界/量程越界拒绝（`TestReject*`）
- 背压与丢弃最旧策略（`TestBackpressureWhenFull`、`TestDropOldestPolicyIsExplicit`）
- 多设备并发共享缓冲（`TestConcurrentDevicesSharedBuffer`、
  `TestConcurrentPutNoCorruption`）
