// Package gateway 串起“解码 -> 版本语义转换 -> 时间戳校验 -> 幂等缓冲 -> 下游投递”。
package gateway

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/protocol"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/sink"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/spool"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/transform"
)

// Logger 是业务日志接口，默认用标准 log，测试可注入带前缀的 logger。
type Logger interface {
	Printf(format string, v ...any)
}

// Stats 汇总一次运行的处理计数，方便测试与演示断言。
type Stats struct {
	Received  int
	Accepted  int
	Duplicate int
	Rejected  int
	Evicted   int
	Backpress int
}

// Config 控制归一管线行为。
type Config struct {
	// MaxFuture / MaxPast 限制设备时间戳相对网关时钟的偏移，越界即拒绝。
	MaxFuture time.Duration
	MaxPast   time.Duration
	Now       func() time.Time // 可注入时钟，保证测试确定性
	Log       Logger
}

// Gateway 是多路设备共享的采集网关：所有设备的帧进入同一 spool，顺序不乱。
type Gateway struct {
	reg  *protocol.Registry
	book *transform.RuleBook
	sp   *spool.Spool
	sink sink.Sink
	cfg  Config

	mu      sync.Mutex
	stats   Stats
	rejects []model.RejectRecord

	notify chan struct{} // 有新待发数据时唤醒转发循环
}

func New(reg *protocol.Registry, book *transform.RuleBook, sp *spool.Spool, sk sink.Sink, cfg Config) *Gateway {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxFuture == 0 {
		cfg.MaxFuture = 5 * time.Minute
	}
	if cfg.MaxPast == 0 {
		cfg.MaxPast = 24 * time.Hour
	}
	if cfg.Log == nil {
		cfg.Log = log.Default()
	}
	return &Gateway{reg: reg, book: book, sp: sp, sink: sk, cfg: cfg, notify: make(chan struct{}, 1)}
}

// Rejects 返回全部拒绝记录快照（含原始报文指纹与原因码，可追溯）。
func (g *Gateway) Rejects() []model.RejectRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]model.RejectRecord, len(g.rejects))
	copy(out, g.rejects)
	return out
}

func (g *Gateway) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

// HandleFrame 处理一帧设备原始报文，是多路设备的唯一入口（内部串行化幂等判断）。
// 正常结束时 error == nil：拒绝不表现为 Go error，而是记录在 Rejects() 中，避免被上游静默吞掉。
// 唯一可能返回的 error 是背压 ErrOverflow（PolicyBlock），上游应稍后重试同一帧。
func (g *Gateway) HandleFrame(frame model.RawFrame) error {
	if frame.ReceivedAt.IsZero() {
		frame.ReceivedAt = g.cfg.Now()
	}
	g.mu.Lock()
	g.stats.Received++
	g.mu.Unlock()

	// 幂等：已投递或在队列里的同设备同序号报文一律判重，不做任何二次处理。
	if g.sp.WasDelivered(frame.Fingerprint()) || g.sp.IsQueued(frame.Fingerprint()) {
		g.cfg.Log.Printf("[business] 重复上报 frame=%s protocol=%s -> 忽略(幂等)", frame.Fingerprint(), frame.Protocol)
		g.mu.Lock()
		g.stats.Duplicate++
		g.mu.Unlock()
		return nil
	}

	dec, reason, detail := g.reg.Decode(frame)
	if reason != "" {
		g.reject(frame, reason, detail)
		return nil
	}

	readings, rec := g.book.Convert(dec)
	if rec != nil {
		g.reject(rec.Frame, rec.Reason, rec.Detail)
		return nil
	}

	now := g.cfg.Now()
	for i := range readings {
		if readings[i].Timestamp.IsZero() {
			g.reject(frame, model.ReasonTimestampOutOfRange, fmt.Sprintf("metric %s: timestamp missing", readings[i].Metric))
			return nil
		}
		if d := readings[i].Timestamp.Sub(now); d > g.cfg.MaxFuture {
			g.reject(frame, model.ReasonTimestampOutOfRange, fmt.Sprintf(
				"metric %s: timestamp %s is %s in the future (max %s)",
				readings[i].Metric, readings[i].Timestamp.Format(time.RFC3339Nano), d, g.cfg.MaxFuture))
			return nil
		}
		if d := now.Sub(readings[i].Timestamp); d > g.cfg.MaxPast {
			g.reject(frame, model.ReasonTimestampOutOfRange, fmt.Sprintf(
				"metric %s: timestamp %s is %s old (max %s)",
				readings[i].Metric, readings[i].Timestamp.Format(time.RFC3339Nano), d, g.cfg.MaxPast))
			return nil
		}
	}

	accepted, evicted, err := g.sp.Enqueue(readings)
	if err != nil {
		g.mu.Lock()
		g.stats.Backpress++
		g.mu.Unlock()
		g.cfg.Log.Printf("[business] 背压 frame=%s 缓冲已满 -> 返回错误,调用方必须重试", frame.Fingerprint())
		return err
	}
	if evicted != nil {
		g.mu.Lock()
		g.stats.Evicted++
		g.mu.Unlock()
		g.cfg.Log.Printf("[business] 丢弃最旧帧 frame=%s reason=%s detail=%s (显式策略,非静默)",
			evicted.FrameID, evicted.Reason, evicted.Detail)
	}
	if !accepted {
		// enqueue 判定为重复（与入口判断之间的竞争兜底）
		g.mu.Lock()
		g.stats.Duplicate++
		g.mu.Unlock()
		return nil
	}
	g.mu.Lock()
	g.stats.Accepted++
	g.mu.Unlock()
	g.cfg.Log.Printf("[business] 入缓冲 frame=%s protocol=%s/%s metrics=%d",
		frame.Fingerprint(), frame.Protocol, frame.Version, len(readings))
	g.kick()
	return nil
}

func (g *Gateway) reject(frame model.RawFrame, reason model.RejectReason, detail string) {
	rec := model.RejectRecord{Frame: frame, Reason: reason, Detail: detail, RejectedAt: g.cfg.Now()}
	g.mu.Lock()
	g.stats.Rejected++
	g.rejects = append(g.rejects, rec)
	g.mu.Unlock()
	g.cfg.Log.Printf("[business] 拒绝 frame=%s protocol=%s raw_len=%d reason=%s detail=%s",
		frame.Fingerprint(), frame.Protocol, len(frame.Raw), reason, detail)
}

func (g *Gateway) kick() {
	select {
	case g.notify <- struct{}{}:
	default:
	}
}

// RunForwarder 持续把缓冲数据投递给下游；下游故障时按间隔重试，数据留在本地。
// 返回的 flush 函数触发一次尽力回放（用于停机前收尾）。
func (g *Gateway) RunForwarder(ctx context.Context, retryInterval time.Duration) (flush func()) {
	go func() {
		ticker := time.NewTicker(retryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-g.notify:
				g.pump()
			case <-ticker.C:
				g.pump()
			}
		}
	}()
	return g.pump
}

func (g *Gateway) pump() {
	for {
		batches := g.sp.Pending()
		if len(batches) == 0 {
			return
		}
		// 总是从最旧帧开始；失败则停止本轮，等待重试，保证 FIFO 不错乱。
		batch := batches[0]
		frameID := batch[0].FrameID
		if err := g.sink.Send(batch); err != nil {
			g.cfg.Log.Printf("[business] 下游不可用 frame=%s sink=%s err=%s -> 保留本地稍后重传",
				frameID, g.sink.Name(), err)
			return
		}
		if err := g.sp.Ack(frameID); err != nil {
			// Send 已成功但 Ack 落盘失败：重启后会重发，下游按帧幂等去重，不会重复生效。
			g.cfg.Log.Printf("[business] ACK落盘失败 frame=%s err=%s (重启后由下游幂等兜底)", frameID, err)
			return
		}
		g.cfg.Log.Printf("[business] 投递成功 frame=%s metrics=%d sink=%s", frameID, len(batch), g.sink.Name())
	}
}

// MustNewDefault 是演示/测试用的一站式装配，失败即 panic。
func MustNewDefault(spoolDir string, capacity int, policy spool.OverflowPolicy, sk sink.Sink, cfg Config) *Gateway {
	reg := protocol.NewRegistry()
	reg.Register(protocol.ModbusDecoder{})
	reg.Register(protocol.OPCUADecoder{})
	sp, err := spool.Open(spoolDir, capacity, policy)
	if err != nil {
		panic(fmt.Sprintf("open spool: %v", err))
	}
	return New(reg, transform.DefaultRules(), sp, sk, cfg)
}

func (g *Gateway) Spool() *spool.Spool { return g.sp }
