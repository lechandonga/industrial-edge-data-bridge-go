// edge-demo 在纯本地环境演示：多协议采集、语义归一、断连缓冲、重连补传、
// 重复上报幂等、版本/字段/时间戳异常拒绝、缓冲背压与显式丢弃。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/gateway"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/simulator"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/sink"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/spool"
)

func main() {
	dir := flag.String("dir", filepath.Join(os.TempDir(), "edge-demo-spool"), "spool directory")
	policyName := flag.String("policy", "drop-oldest", "overflow policy: block | drop-oldest")
	capacity := flag.Int("capacity", 4, "buffer capacity in frames")
	flag.Parse()

	_ = os.RemoveAll(*dir) // 演示每次从干净状态开始

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	policy := spool.PolicyDropOldest
	if *policyName == "block" {
		policy = PolicyBlock
	}

	downstream := sink.NewLocalSink("local-analytics", false) // 开场下游不可用
	gw := gateway.MustNewDefault(*dir, *capacity, policy, downstream, gateway.Config{
		MaxFuture: 5 * time.Minute, MaxPast: 24 * time.Hour, Log: logger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flush := gw.RunForwarder(ctx, 200*time.Millisecond)

	now := time.Now().UTC()
	mb1 := simulator.New("plc-line1", "modbus", "1", func() time.Time { return now })
	mb2 := simulator.New("plc-line2", "modbus", "2", func() time.Time { return now })
	opc := simulator.New("opc-main", "opcua", "1", func() time.Time { return now })

	must := func(f model.RawFrame, ok bool) model.RawFrame {
		if !ok {
			log.Fatal("device unexpectedly offline")
		}
		return f
	}

	// 1) 下游离线时多设备正常采集：数据进入本地缓冲。
	f1 := must(mb1.Poll(simulator.ModbusV1PayloadWithDevice("plc-line1", 251, 1013, now.Unix())))
	gw.HandleFrame(f1)
	f2 := must(mb2.Poll(simulator.ModbusV2PayloadWithDevice("plc-line2", 25.1, 0.1013, now.UnixMilli())))
	gw.HandleFrame(f2)
	f3 := must(opc.Poll(simulator.OPCUAV1PayloadWithDevice("opc-main", "312", "101300", now.Format(time.RFC3339))))
	gw.HandleFrame(f3)

	// 2) 设备断连期间无数据；重连后重放上一帧 -> 幂等去重。
	mb1.Disconnect()
	if _, ok := mb1.Poll(simulator.ModbusV1PayloadWithDevice("plc-line1", 260, 1014, now.Unix())); ok {
		log.Fatal("expected device offline")
	}
	logger.Printf("[business] plc-line1 断连,采集中跳过")
	mb1.Reconnect()
	dup, _ := mb1.ReplayLast()
	gw.HandleFrame(dup)

	// 3) 异常报文：未知版本 / 字段类型异常 / 时间戳越界 / 必填缺失 / 量程越界。
	bad := []model.RawFrame{
		simulator.Raw("plc-line1", "modbus", "9", 100, []byte(`{"device_id":"plc-line1","ts":1,"regs":{"40001":1}}`), now),
		simulator.Raw("plc-line2", "modbus", "2", 101, []byte(`{"device_id":"plc-line2","ts":1,"regs":{"40001":"oops"}}`), now),
		simulator.Raw("opc-main", "opcua", "1", 102, []byte(`{"device_id":"opc-main","ts":"2099-01-01T00:00:00Z","nodes":{"temperature":{"value":251},"pressure":{"value":101300}}}`), now),
		simulator.Raw("opc-main", "opcua", "1", 103, []byte(`{"device_id":"opc-main","ts":"2026-01-01T00:00:00Z","nodes":{"temperature":{"value":1}}}`), now),
		simulator.Raw("plc-line2", "modbus", "2", 104, []byte(`{"device_id":"plc-line2","ts":1,"regs":{"40001":9999,"40002":0.1}}`), now),
		simulator.Raw("plc-x", "baccarat", "1", 105, []byte(`{}`), now),
	}
	for _, fr := range bad {
		gw.HandleFrame(fr)
	}

	// 4) 缓冲上限策略：继续入队，观察 block 背压或 drop-oldest 审计记录。
	for i := 0; i < 4; i++ {
		fr := must(mb2.Poll(simulator.ModbusV2PayloadWithDevice("plc-line2", 26.0+float64(i)*0.1, 0.1010, now.UnixMilli())))
		if err := gw.HandleFrame(fr); err != nil {
			logger.Printf("[business] 入队被背压拒绝(可重试): %v", err)
		}
	}

	time.Sleep(150 * time.Millisecond)

	// 5) 下游恢复：FIFO 回放补传，重复帧不会重复入库。
	logger.Printf("[business] 下游恢复,开始补传")
	downstream.SetAvailable(true)
	flush()
	time.Sleep(300 * time.Millisecond)

	st := gw.Stats()
	logger.Printf("[result] 统计 received=%d accepted=%d duplicate=%d rejected=%d evicted=%d backpress=%d",
		st.Received, st.Accepted, st.Duplicate, st.Rejected, st.Evicted, st.Backpress)
	logger.Printf("[result] 下游实际收到读数 %d 条（按帧幂等）", len(downstream.Received()))
	for _, r := range downstream.Received() {
		logger.Printf("[result] 归一数据 device=%s metric=%s value=%.4f unit=%s proto=%s/%s frame=%s",
			r.DeviceID, r.Metric, r.Value, r.Unit, r.Protocol, r.Version, r.FrameID)
	}
	for _, rj := range gw.Rejects() {
		logger.Printf("[result] 拒绝记录 frame=%s reason=%s detail=%s raw=%s",
			rj.Frame.Fingerprint(), rj.Reason, rj.Detail, string(rj.Frame.Raw))
	}
	for _, ev := range gw.Spool().Evictions() {
		logger.Printf("[result] 显式丢弃 frame=%s reason=%s detail=%s", ev.FrameID, ev.Reason, ev.Detail)
	}
}

const PolicyBlock = spool.OverflowPolicy(0) // 别名以保持 main 内引用直观
