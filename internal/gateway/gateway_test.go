package gateway_test

import (
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/buffer"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/gateway"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/protocol"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/sim"
)

func newGateway(t *testing.T, dir string, capacity int, policy buffer.FullPolicy, sink *sim.Sink) *gateway.Gateway {
	t.Helper()
	buf, err := buffer.Open(dir, capacity, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { buf.Close() })
	return gateway.New(protocol.DefaultRegistry(), buf, sink, log.New(io.Discard, "", 0))
}

func nowMillis() int64 { return time.Now().UnixMilli() }

func TestDownstreamOutageAndReplay(t *testing.T) {
	t.Log("业务场景: 下游不可用 - 本地缓冲, 恢复后按序补传, 不丢不重")
	sink := sim.NewSink()
	gw := newGateway(t, t.TempDir(), 100, buffer.PolicyBackpressure, sink)
	dev := sim.NewDevice("dev-1", "modsim", "v1", map[string]float64{"temp": 255, "pressure": 100}, nowMillis())

	sink.SetAvailable(false)
	var ids []string
	for i := 0; i < 5; i++ {
		raw, err := dev.Report("")
		if err != nil {
			t.Fatal(err)
		}
		if err := gw.IngestRaw("modsim", raw); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := gw.FlushPending(); err == nil || n != 0 {
		t.Fatalf("flush during outage should fail with 0 sent, got n=%d err=%v", n, err)
	}
	t.Log("下游不可用期间 5 条数据已落盘缓冲, 补传前发送 0 条")

	sink.SetAvailable(true)
	n, err := gw.FlushPending()
	if err != nil || n != 5 {
		t.Fatalf("replay want 5 sent, got n=%d err=%v", n, err)
	}
	for i, b := range sink.Received() {
		ids = append(ids, b.BatchID)
		for _, p := range b.Points {
			if p.Metric == "temperature" && p.Value != 25.5 {
				t.Fatalf("batch %d value corrupted: %v", i, p.Value)
			}
		}
	}
	if sink.DuplicateCount() != 0 {
		t.Fatal("no duplicates allowed downstream")
	}
	t.Logf("用例通过: 恢复后补传 %d 条, 顺序=%v, 无重复", n, ids)
}

func TestDuplicateReportNotDeliveredTwice(t *testing.T) {
	t.Log("业务场景: 设备重复上报 - 同一批次只下发一次")
	sink := sim.NewSink()
	gw := newGateway(t, t.TempDir(), 100, buffer.PolicyBackpressure, sink)
	dev := sim.NewDevice("dev-2", "modsim", "v1", map[string]float64{"temp": 200, "pressure": 50}, nowMillis())

	raw, err := dev.Report("fixed-batch-1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := gw.IngestRaw("modsim", raw); err != nil {
			t.Fatal(err)
		}
	}
	n, err := gw.FlushPending()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(sink.Received()) != 1 {
		t.Fatalf("duplicate report must be delivered once, sent=%d received=%d", n, len(sink.Received()))
	}
	t.Log("用例通过: 同一批次上报 3 次, 下游仅收到 1 次")
}

func TestIncompatibleVersionRejected(t *testing.T) {
	t.Log("业务场景: 协议版本不兼容 - 明确拒绝且不影响其他设备数据")
	sink := sim.NewSink()
	gw := newGateway(t, t.TempDir(), 100, buffer.PolicyBackpressure, sink)
	bad := sim.NewDevice("dev-bad", "modsim", "v9", map[string]float64{"temp": 1, "pressure": 1}, nowMillis())
	good := sim.NewDevice("dev-good", "modsim", "v2", map[string]float64{"temp": 77, "pressure": 5}, nowMillis())

	rawBad, _ := bad.Report("")
	err := gw.IngestRaw("modsim", rawBad)
	re, ok := model.IsReject(err)
	if !ok || re.Reason != model.ReasonUnknownVersion {
		t.Fatalf("want unknown_version reject, got %v", err)
	}
	rawGood, _ := good.Report("")
	if err := gw.IngestRaw("modsim", rawGood); err != nil {
		t.Fatal(err)
	}
	if _, err := gw.FlushPending(); err != nil {
		t.Fatal(err)
	}
	got := sink.Received()
	if len(got) != 1 {
		t.Fatalf("77F must normalize to 25C, got %+v", got)
	}
	var tempVal float64
	for _, p := range got[0].Points {
		if p.Metric == "temperature" {
			tempVal = p.Value
		}
	}
	if tempVal != 25 {
		t.Fatalf("77F must normalize to 25C, got %v", tempVal)
	}
	if len(gw.Rejects()) != 1 {
		t.Fatalf("reject must be recorded, got %d", len(gw.Rejects()))
	}
	t.Logf("用例通过: v9 被拒绝(原因=%s, 批次=%s), v2 设备 77F -> 25C 正常下发",
		re.Reason, re.BatchID)
}

func TestConcurrentDevicesSharedBuffer(t *testing.T) {
	t.Log("业务场景: 多设备并发采集共享缓冲 - 数据不错乱(竞态检测)")
	sink := sim.NewSink()
	gw := newGateway(t, t.TempDir(), 10000, buffer.PolicyBackpressure, sink)
	var wg sync.WaitGroup
	for d := 0; d < 4; d++ {
		dev := sim.NewDevice(fmt.Sprintf("dev-%d", d), "modsim", "v1",
			map[string]float64{"temp": float64(100 + d), "pressure": 10}, nowMillis())
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				raw, err := dev.Report("")
				if err != nil {
					t.Error(err)
					return
				}
				if err := gw.IngestRaw("modsim", raw); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	n, err := gw.FlushPending()
	if err != nil {
		t.Fatal(err)
	}
	if n != 200 || len(sink.Received()) != 200 || sink.DuplicateCount() != 0 {
		t.Fatalf("want 200 delivered exactly once, sent=%d received=%d dup=%d",
			n, len(sink.Received()), sink.DuplicateCount())
	}
	for _, b := range sink.Received() {
		if len(b.Points) != 2 || b.Points[0].DeviceID == "" {
			t.Fatalf("corrupted batch %+v", b)
		}
	}
	t.Log("用例通过: 4 设备并发各 50 条, 200 条全部恰好送达一次")
}

func TestRestartMidOutageKeepsData(t *testing.T) {
	t.Log("业务场景: 断连期间进程重启 - 缓冲数据恢复后继续补传")
	dir := t.TempDir()
	sink := sim.NewSink()
	sink.SetAvailable(false)
	gw := newGateway(t, dir, 100, buffer.PolicyBackpressure, sink)
	dev := sim.NewDevice("dev-r", "jsonline", "v1", map[string]float64{"temp": 30, "humidity": 60}, nowMillis())
	for i := 0; i < 3; i++ {
		raw, _ := dev.Report("")
		if err := gw.IngestRaw("jsonline", raw); err != nil {
			t.Fatal(err)
		}
	}

	// 模拟进程重启: 同一目录重新打开缓冲并新建网关。
	gw2 := newGateway(t, dir, 100, buffer.PolicyBackpressure, sink)
	sink.SetAvailable(true)
	n, err := gw2.FlushPending()
	if err != nil || n != 3 {
		t.Fatalf("after restart want 3 replayed, got n=%d err=%v", n, err)
	}
	if sink.DuplicateCount() != 0 {
		t.Fatal("no duplicates allowed")
	}
	t.Log("用例通过: 重启后 3 条缓冲数据全部补传, 无重复")
}
