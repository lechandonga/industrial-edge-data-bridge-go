package buffer

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

func mkBatch(id string) *model.Batch {
	return &model.Batch{
		BatchID:  id,
		Protocol: "modsim",
		Version:  "v1",
		Raw:      []byte(`{"batch_id":"` + id + `"}`),
		Points: []model.Point{{DeviceID: "dev", Metric: "temperature", Value: 1,
			Unit: "celsius", Timestamp: time.Now()}},
	}
}

func TestRestartRecovery(t *testing.T) {
	t.Log("业务场景: 缓冲持久化 - 进程重启后未确认数据不丢、已确认数据不重发")
	dir := t.TempDir()
	buf, err := Open(dir, 100, PolicyBackpressure)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, err := buf.Put(mkBatch(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := buf.Ack("a"); err != nil {
		t.Fatal(err)
	}
	buf.Close()

	reopened, err := Open(dir, 100, PolicyBackpressure)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	pending := reopened.Pending(10)
	if len(pending) != 2 || pending[0].BatchID != "b" || pending[1].BatchID != "c" {
		t.Fatalf("after restart want pending [b c], got %v", pending)
	}
	res, err := reopened.Put(mkBatch("a"))
	if err != nil || !res.Duplicate {
		t.Fatalf("acked batch re-put after restart must be deduped, got %+v err=%v", res, err)
	}
	t.Logf("用例通过: 重启后待补传=%d 条, 已确认批次重复上报被去重", len(pending))
}

func TestDuplicateReportDedup(t *testing.T) {
	t.Log("业务场景: 重复上报 - 同一 batch_id 重复提交只保留一份")
	buf, err := Open(t.TempDir(), 100, PolicyBackpressure)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close()
	for i := 0; i < 3; i++ {
		res, err := buf.Put(mkBatch("dup-1"))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && res.Duplicate {
			t.Fatal("first put must not be a duplicate")
		}
		if i > 0 && !res.Duplicate {
			t.Fatal("repeated put must be flagged duplicate")
		}
	}
	if buf.Len() != 1 {
		t.Fatalf("want 1 pending, got %d", buf.Len())
	}
	t.Log("用例通过: 3 次重复上报, 缓冲中仅 1 条")
}

func TestBackpressureWhenFull(t *testing.T) {
	t.Log("业务场景: 缓冲区满(背压策略) - 明确返回错误, 不静默丢弃")
	buf, err := Open(t.TempDir(), 2, PolicyBackpressure)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close()
	buf.Put(mkBatch("x"))
	buf.Put(mkBatch("y"))
	_, err = buf.Put(mkBatch("z"))
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("want ErrBufferFull, got %v", err)
	}
	if buf.Len() != 2 {
		t.Fatalf("existing entries must be kept, got %d", buf.Len())
	}
	t.Log("用例通过: 容量=2 时第 3 条返回 ErrBufferFull, 已有数据保留")
}

func TestDropOldestPolicyIsExplicit(t *testing.T) {
	t.Log("业务场景: 缓冲区满(丢弃最旧策略) - 丢弃数量显式上报, 不静默")
	buf, err := Open(t.TempDir(), 2, PolicyDropOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close()
	buf.Put(mkBatch("x"))
	buf.Put(mkBatch("y"))
	res, err := buf.Put(mkBatch("z"))
	if err != nil {
		t.Fatal(err)
	}
	if res.DroppedOldest != 1 {
		t.Fatalf("want 1 explicit drop, got %d", res.DroppedOldest)
	}
	pending := buf.Pending(10)
	if pending[0].BatchID != "y" || pending[1].BatchID != "z" {
		t.Fatalf("oldest must be evicted first, got %v", pending)
	}
	t.Logf("用例通过: 显式丢弃 %d 条最旧数据, 缓冲=[y z]", res.DroppedOldest)
}

func TestConcurrentPutNoCorruption(t *testing.T) {
	t.Log("业务场景: 多路并发写入共享缓冲 - 数据不错乱(竞态检测)")
	buf, err := Open(t.TempDir(), 10000, PolicyBackpressure)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if _, err := buf.Put(mkBatch(fmt.Sprintf("w%d-%d", w, i))); err != nil {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Wait()
	if buf.Len() != 800 {
		t.Fatalf("want 800 pending, got %d", buf.Len())
	}
	seen := map[string]bool{}
	for _, b := range buf.Pending(1000) {
		if seen[b.BatchID] {
			t.Fatalf("duplicate id in buffer: %s", b.BatchID)
		}
		seen[b.BatchID] = true
	}
	t.Log("用例通过: 8 路并发写入 800 条, 无丢失无重复")
}
