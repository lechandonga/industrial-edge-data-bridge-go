# industrial-edge-data-bridge-go

纯本地可复现的边缘网关数据采集与协议转换样板（Go，仅标准库）：

- 多协议采集（模拟 Modbus v1/v2、OPC-UA v1）并归一到统一数据模型；
- 字段类型异常、时间戳越界、必填缺失、量程越界、未知协议/版本等场景
  一律给出**可区分原因码**的拒绝记录，且保留**原始报文**可追溯，不静默丢弃；
- 设备断连/下游不可用时数据落本地 WAL 缓冲，恢复后 FIFO 补传；
  进程重启、重复上报均做到不丢不重（`deviceID:seq` 幂等 + 下游帧级幂等）；
- 缓冲上限策略显式可选：`Block` 背压（错误可重试）或 `DropOldest`（审计可查）；
- 按协议版本做单位/缩放/量程语义转换，未知版本明确拒绝而不是猜测；
- 多路设备并发共享同一缓冲区，`-race` 下稳定通过。

## 快速开始

```bash
go run ./cmd/edge-demo -policy drop-oldest -capacity 4
go run ./cmd/edge-demo -policy block -capacity 2
```

演示覆盖：多协议归一、设备断连/重连、重放去重、下游离线补传、
未知版本/字段类型/时间戳/必填/量程拒绝、缓冲背压与显式丢弃。

## 运行测试

```bash
make test-race   # 需要 cgo（gcc）；输出每个业务用例的详细日志
make test
```

## 代码结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 统一模型 `Reading`、原始帧 `RawFrame`、拒绝记录与原因码 |
| `internal/protocol` | 协议注册表与 Modbus/OPC-UA 解码器（严格类型校验） |
| `internal/transform` | `协议@版本` 语义规则表（单位、缩放、偏移、量程、时间戳解释） |
| `internal/spool` | 有界持久化 FIFO 缓冲（WAL、重启恢复、背压/丢弃策略） |
| `internal/sink` | 下游接口与本地模拟接收端（可注入离线/瞬时故障、帧级幂等） |
| `internal/gateway` | 管线编排：解码→转换→校验→去重→缓冲→投递重试 |
| `internal/simulator` | 本地模拟设备（断连/重连/重放上一帧、各类载荷构造） |
| `cmd/edge-demo` | 本地演示入口 |

详细设计（协议报文格式、版本规则、WAL 与去重、验证清单）见
[`docs/DESIGN.md`](docs/DESIGN.md)。
