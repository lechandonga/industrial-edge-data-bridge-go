package gateway_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/gateway"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/protocol"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/simulator"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/sink"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/spool"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/transform"
)

func jsonish(format string, args ...any) string { return fmt.Sprintf(format, args...) }

func testLog(t *testing.T) *log.Logger {
	t.Helper()
	buf := &safeBuffer{}
	t.Cleanup(func() {
		t.Logf("业务日志:\n%s", strings.TrimSpace(buf.String()))
	})
	return log.New(buf, "["+t.Name()+"] ", log.Lmicroseconds)
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func newTestGW(sp *spool.Spool, sk sink.Sink, now time.Time, t *testing.T) *gateway.Gateway {
	reg := protocol.NewRegistry()
	reg.Register(protocol.ModbusDecoder{})
	reg.Register(protocol.OPCUADecoder{})
	return gateway.New(reg, transform.DefaultRules(), sp, sk, gateway.Config{
		MaxFuture: 5 * time.Minute,
		MaxPast:   24 * time.Hour,
		Now:       func() time.Time { return now },
		Log:       testLog(t),
	})
}

func setup(t *testing.T, capacity int, policy spool.OverflowPolicy, sk *sink.LocalSink) (*gateway.Gateway, time.Time, func()) {
	t.Helper()
	now := time.Date(2026, 10, 7, 4, 0, 0, 0, time.UTC)
	sp, err := spool.Open(filepath.Join(t.TempDir(), "spool"), capacity, policy)
	if err != nil {
		t.Fatal(err)
	}
	gw := newTestGW(sp, sk, now, t)
	ctx, cancel := context.WithCancel(context.Background())
	flush := gw.RunForwarder(ctx, 20*time.Millisecond)
	return gw, now, func() {
		cancel()
		flush()
		sp.Close()
	}
}

func modbusV1(id string, seq uint64, tempDeci, kpa float64, now time.Time) model.RawFrame {
	return simulator.Raw(id, "modbus", "1", seq,
		[]byte(jsonish(`{"device_id":%q,"ts":%d,"regs":{"40001":%g,"40002":%g}}`,
			id, now.Unix(), tempDeci, kpa)), now)
}

func reasons(rr []model.RejectRecord) map[string]model.RejectReason {
	m := map[string]model.RejectReason{}
	for _, r := range rr {
		m[r.Frame.Fingerprint()] = r.Reason
	}
	return m
}

func assertApprox(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("value=%v want %v", got, want)
	}
}

// TestDisconnectReconnectAndReplay 业务:断连不产数,重连后重放帧按幂等忽略。
func TestDisconnectReconnectAndReplay(t *testing.T) {
	sk := sink.NewLocalSink("s", true)
	gw, now, done := setup(t, 100, spool.PolicyBlock, sk)
	defer done()

	dev := simulator.New("plc-1", "modbus", "1", func() time.Time { return now })
	f, ok := dev.Poll(simulator.ModbusV1PayloadWithDevice("plc-1", 251, 1013, now.Unix()))
	if !ok {
		t.Fatal("expected online")
	}
	if err := gw.HandleFrame(f); err != nil {
		t.Fatalf("handle: %v", err)
	}

	dev.Disconnect()
	if _, ok := dev.Poll(simulator.ModbusV1PayloadWithDevice("plc-1", 260, 1014, now.Unix())); ok {
		t.Fatal("断连期间不应产出数据")
	}
	dev.Reconnect()

	dup, ok := dev.ReplayLast()
	if !ok {
		t.Fatal("replay expected")
	}
	if err := gw.HandleFrame(dup); err != nil {
		t.Fatalf("replay: %v", err)
	}

	time.Sleep(80 * time.Millisecond)
	if gw.Stats().Duplicate != 1 {
		t.Fatalf("duplicate=%d want 1", gw.Stats().Duplicate)
	}
	if got := len(sk.Received()); got != 2 {
		t.Fatalf("sink readings=%d want 2 (一帧两点只生效一次)", got)
	}
}

// TestSpoolReplayAfterOutage 业务:下游断时数据落本地,恢复后 FIFO 补传不丢,ACK后重报不重。
func TestSpoolReplayAfterOutage(t *testing.T) {
	sk := sink.NewLocalSink("s", false)
	gw, now, done := setup(t, 100, spool.PolicyBlock, sk)
	defer done()

	for i := 1; i <= 3; i++ {
		if err := gw.HandleFrame(modbusV1("plc-1", uint64(i), 250+float64(i), 1013, now)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if len(sk.Received()) != 0 {
		t.Fatalf("下游离线不应收到数据, got %d", len(sk.Received()))
	}

	sk.SetAvailable(true)
	time.Sleep(150 * time.Millisecond)
	got := sk.Received()
	if len(got) != 6 {
		t.Fatalf("恢复后应补传 6 条读数, got %d", len(got))
	}
	if got[0].FrameID != "plc-1:1" || got[len(got)-1].FrameID != "plc-1:3" {
		t.Fatalf("FIFO 顺序错乱: first=%s last=%s", got[0].FrameID, got[len(got)-1].FrameID)
	}

	if err := gw.HandleFrame(modbusV1("plc-1", 1, 251, 1013, now)); err != nil {
		t.Fatalf("dup after ack: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if len(sk.Received()) != 6 {
		t.Fatalf("ACK 后重复帧不得重复入库, got %d", len(sk.Received()))
	}
}

// TestRestartNoLossNoDuplicate 业务:进程重启重放 WAL,不丢也不重。
func TestRestartNoLossNoDuplicate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	now := time.Date(2026, 10, 7, 4, 0, 0, 0, time.UTC)
	sk := sink.NewLocalSink("s", false)

	sp1, err := spool.Open(dir, 0, spool.PolicyBlock)
	if err != nil {
		t.Fatal(err)
	}
	gw1 := newTestGW(sp1, sk, now, t)
	if err := gw1.HandleFrame(modbusV1("plc-1", 1, 251, 1013, now)); err != nil {
		t.Fatal(err)
	}
	if err := gw1.HandleFrame(modbusV1("plc-1", 2, 252, 1013, now)); err != nil {
		t.Fatal(err)
	}
	sp1.Close()

	sp2, err := spool.Open(dir, 0, spool.PolicyBlock)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(sp2.Pending()); n != 2 {
		t.Fatalf("重启后待发帧=%d want 2", n)
	}
	gw2 := newTestGW(sp2, sk, now, t)
	sk.SetAvailable(true)
	ctx, cancel := context.WithCancel(context.Background())
	flush := gw2.RunForwarder(ctx, 20*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	if got := len(sk.Received()); got != 4 {
		t.Fatalf("重启补传读数=%d want 4", got)
	}
	flush()
	cancel()
	sp2.Close()

	sp3, err := spool.Open(dir, 0, spool.PolicyBlock)
	if err != nil {
		t.Fatal(err)
	}
	defer sp3.Close()
	if n := len(sp3.Pending()); n != 0 {
		t.Fatalf("ACK 后重启待发应为 0, got %d", n)
	}
	if !sp3.WasDelivered("plc-1:1") {
		t.Fatal("已投递指纹应在重启后保留用于去重")
	}
}

// TestDuplicateWhilePending 业务:未投递时同序号重复上报不重复入队。
func TestDuplicateWhilePending(t *testing.T) {
	sk := sink.NewLocalSink("s", false)
	gw, now, done := setup(t, 100, spool.PolicyBlock, sk)
	defer done()

	f := modbusV1("plc-1", 1, 251, 1013, now)
	if err := gw.HandleFrame(f); err != nil {
		t.Fatal(err)
	}
	if err := gw.HandleFrame(f); err != nil {
		t.Fatal(err)
	}
	if n := len(gw.Spool().Pending()); n != 1 {
		t.Fatalf("pending frames=%d want 1", n)
	}
	if gw.Stats().Duplicate != 1 {
		t.Fatalf("duplicate count=%d want 1", gw.Stats().Duplicate)
	}
}

// TestVersionSemanticsAndUnknown 业务:各版本单位/缩放正确归一,未知版本显式拒绝。
func TestVersionSemanticsAndUnknown(t *testing.T) {
	sk := sink.NewLocalSink("s", true)
	gw, now, done := setup(t, 100, spool.PolicyBlock, sk)
	defer done()

	gw.HandleFrame(modbusV1("plc-1", 1, 251, 1013, now))
	gw.HandleFrame(simulator.Raw("plc-2", "modbus", "2", 1,
		[]byte(jsonish(`{"device_id":"plc-2","ts":%d,"regs":{"40001":25.1,"40002":0.1013}}`, now.UnixMilli())), now))
	gw.HandleFrame(simulator.Raw("opc-1", "opcua", "1", 1,
		[]byte(jsonish(`{"device_id":"opc-1","ts":%q,"nodes":{"temperature":{"value":312,"quality":"GOOD"},"pressure":{"value":101300,"quality":"GOOD"}}}`,
			now.Format(time.RFC3339))), now))
	gw.HandleFrame(simulator.Raw("plc-1", "modbus", "7", 2,
		[]byte(`{"device_id":"plc-1","ts":1,"regs":{"40001":1,"40002":2}}`), now))

	time.Sleep(100 * time.Millisecond)
	got := sk.Received()
	if len(got) != 6 {
		t.Fatalf("accepted readings=%d want 6", len(got))
	}
	val := map[string]float64{}
	unit := map[string]string{}
	for _, r := range got {
		val[r.DeviceID+"/"+r.Metric] = r.Value
		unit[r.Metric] = r.Unit
	}
	assertApprox(t, val["plc-1/temperature"], 25.1)
	assertApprox(t, val["plc-1/pressure"], 1013)
	assertApprox(t, val["plc-2/temperature"], 25.1)
	assertApprox(t, val["plc-2/pressure"], 1013)
	assertApprox(t, val["opc-1/temperature"], 31.2)
	assertApprox(t, val["opc-1/pressure"], 1013)
	if unit["temperature"] != "C" || unit["pressure"] != "hPa" {
		t.Fatalf("归一单位错误: %v", unit)
	}
	if reasons(gw.Rejects())["plc-1:2"] != model.ReasonUnsupportedVersion {
		t.Fatalf("未知版本应拒绝, got %v", reasons(gw.Rejects()))
	}
}

// TestRejectReasons 业务:各类异常给出可区分原因,拒绝记录保留原始报文可追溯。
func TestRejectReasons(t *testing.T) {
	sk := sink.NewLocalSink("s", true)
	gw, now, done := setup(t, 100, spool.PolicyBlock, sk)
	defer done()

	cases := []struct {
		name   string
		frame  model.RawFrame
		reason model.RejectReason
	}{
		{"字段类型异常", simulator.Raw("d", "modbus", "2", 1,
			[]byte(`{"device_id":"d","ts":1,"regs":{"40001":"hot","40002":0.1}}`), now), model.ReasonFieldType},
		{"时间戳在未来", simulator.Raw("d", "opcua", "1", 2,
			[]byte(`{"device_id":"d","ts":"2099-01-01T00:00:00Z","nodes":{"temperature":{"value":251},"pressure":{"value":101300}}}`), now), model.ReasonTimestampOutOfRange},
		{"时间戳过旧", simulator.Raw("d", "opcua", "1", 3,
			[]byte(`{"device_id":"d","ts":"2000-01-01T00:00:00Z","nodes":{"temperature":{"value":251},"pressure":{"value":101300}}}`), now), model.ReasonTimestampOutOfRange},
		{"RFC3339格式坏", simulator.Raw("d", "opcua", "1", 4,
			[]byte(`{"device_id":"d","ts":12345,"nodes":{"temperature":{"value":251},"pressure":{"value":101300}}}`), now), model.ReasonFieldType},
		{"必填测点缺失", simulator.Raw("d", "modbus", "1", 5,
			[]byte(jsonish(`{"device_id":"d","ts":%d,"regs":{"40001":251}}`, now.Unix())), now), model.ReasonMissingField},
		{"设备ID缺失", simulator.Raw("", "modbus", "1", 6,
			[]byte(`{"device_id":"","ts":1,"regs":{"40001":251,"40002":1013}}`), now), model.ReasonMissingField},
		{"量程越界", simulator.Raw("d", "modbus", "2", 7,
			[]byte(`{"device_id":"d","ts":1,"regs":{"40001":9999,"40002":0.1}}`), now), model.ReasonValueOutOfRange},
		{"报文畸形", simulator.Raw("d", "modbus", "1", 8,
			[]byte(`{not-json`), now), model.ReasonMalformedFrame},
		{"未知协议", simulator.Raw("d", "baccarat", "1", 9,
			[]byte(`{}`), now), model.ReasonUnknownProtocol},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := gw.HandleFrame(c.frame); err != nil {
				t.Fatalf("拒绝不应表现为 error: %v", err)
			}
		})
	}
	time.Sleep(80 * time.Millisecond)
	rj := reasons(gw.Rejects())
	for _, c := range cases {
		if rj[c.frame.Fingerprint()] != c.reason {
			t.Errorf("%s: reason=%v want %v", c.name, rj[c.frame.Fingerprint()], c.reason)
		}
	}
	// 拒绝记录必须能追溯到原始报文
	recs := gw.Rejects()
	for _, r := range recs {
		if len(r.Frame.Raw) == 0 {
			t.Errorf("拒绝记录缺少原始报文: %+v", r)
		}
	}
	if len(sk.Received()) != 0 {
		t.Fatalf("所有异常帧都不得进入下游, got %d", len(sk.Received()))
	}
}

// TestOverflowPolicies 业务:缓冲上限下 Block 背压可重试;DropOldest 显式丢弃有审计。
func TestOverflowPolicies(t *testing.T) {
	t.Run("Block背压", func(t *testing.T) {
		sk := sink.NewLocalSink("s", false)
		gw, now, done := setup(t, 2, spool.PolicyBlock, sk)
		defer done()
		if err := gw.HandleFrame(modbusV1("d", 1, 251, 1013, now)); err != nil {
			t.Fatal(err)
		}
		if err := gw.HandleFrame(modbusV1("d", 2, 252, 1013, now)); err != nil {
			t.Fatal(err)
		}
		err := gw.HandleFrame(modbusV1("d", 3, 253, 1013, now))
		if !errors.Is(err, spool.ErrOverflow) {
			t.Fatalf("满缓冲应返回 ErrOverflow, got %v", err)
		}
		// 下游恢复、排空后,被背压的帧重试应成功
		sk.SetAvailable(true)
		time.Sleep(150 * time.Millisecond)
		if err := gw.HandleFrame(modbusV1("d", 3, 253, 1013, now)); err != nil {
			t.Fatalf("背压后重试应成功: %v", err)
		}
	})

	t.Run("DropOldest显式丢弃", func(t *testing.T) {
		sk := sink.NewLocalSink("s", false)
		gw, now, done := setup(t, 2, spool.PolicyDropOldest, sk)
		defer done()
		_ = gw.HandleFrame(modbusV1("d", 1, 251, 1013, now))
		_ = gw.HandleFrame(modbusV1("d", 2, 252, 1013, now))
		_ = gw.HandleFrame(modbusV1("d", 3, 253, 1013, now))
		ev := gw.Spool().Evictions()
		if len(ev) != 1 || ev[0].FrameID != "d:1" {
			t.Fatalf("应显式丢弃最旧帧 d:1, got %+v", ev)
		}
		if ev[0].Reason != model.ReasonBufferOverflow {
			t.Fatalf("丢弃原因=%v want BUFFER_OVERFLOW", ev[0].Reason)
		}
	})
}

// TestTransientSinkFailure 业务:下游瞬时失败不丢数据,自动重试且不重复。
func TestTransientSinkFailure(t *testing.T) {
	sk := sink.NewLocalSink("s", true)
	sk.FailNextN(2)
	gw, now, done := setup(t, 100, spool.PolicyBlock, sk)
	defer done()

	if err := gw.HandleFrame(modbusV1("d", 1, 251, 1013, now)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	got := sk.Received()
	if len(got) != 2 {
		t.Fatalf("瞬时失败后应最终送达 2 条, got %d", len(got))
	}
}

// TestConcurrentDevicesSharedSpool 业务:多路设备并发共享缓冲,数据不错乱、不丢失、不重复。
// 使用 -race 运行可检测数据竞争。
func TestConcurrentDevicesSharedSpool(t *testing.T) {
	sk := sink.NewLocalSink("s", false) // 先离线,制造积压
	gw, now, done := setup(t, 0, spool.PolicyBlock, sk)
	defer done()

	const devices = 8
	const framesPerDevice = 25
	var wg sync.WaitGroup
	for d := 0; d < devices; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			id := fmt.Sprintf("dev-%d", d)
			for i := 1; i <= framesPerDevice; i++ {
				f := modbusV1(id, uint64(i), 250+float64(i%10), 1013, now)
				if err := gw.HandleFrame(f); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(d)
	}
	wg.Wait()

	sk.SetAvailable(true)
	// 等待全部投递
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(gw.Spool().Pending()) == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := len(gw.Spool().Pending()); n != 0 {
		t.Fatalf("仍有 %d 帧未投递", n)
	}

	got := sk.Received()
	if want := devices * framesPerDevice * 2; len(got) != want {
		t.Fatalf("送达读数=%d want %d (每帧2测点)", len(got), want)
	}
	seen := map[string]int{}
	for _, r := range got {
		seen[r.FrameID]++
	}
	for d := 0; d < devices; d++ {
		for i := 1; i <= framesPerDevice; i++ {
			id := fmt.Sprintf("dev-%d:%d", d, i)
			if seen[id] != 2 { // 每帧 2 个测点,各出现一次
				t.Fatalf("帧 %s 出现 %d 条读数, want 2 (不错乱/不重复)", id, seen[id])
			}
		}
	}
}
