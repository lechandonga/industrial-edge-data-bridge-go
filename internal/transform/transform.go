// Package transform 按“协议+版本”的规则表把解码后的报文归一为统一模型。
// 规则缺失（未知版本/未知协议）一律显式拒绝，不做任何猜测性转换。
package transform

import (
	"fmt"
	"math"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/protocol"
)

// FieldRule 描述单个测点在该版本下的语义。
type FieldRule struct {
	SourceKey string
	Metric    string
	// 归一公式: normalized = raw*Scale + Offset；Unit 为归一后的标准单位。
	Scale  float64
	Offset float64
	Unit   string
	// Min/Max 是归一值允许量程，越界拒绝。
	Min float64
	Max float64
}

// Rule 是某协议某版本的完整转换规则。
type Rule struct {
	Protocol string
	Version  string
	// Required 为 true 的测点缺失时拒绝整帧。
	Fields []FieldRule
	// parseTime 解释解码阶段保留下来的时间戳原始值。
	parseTime func(dec *protocol.DecodedFrame) (time.Time, error)
}

// RuleBook 是不可重复注册的版本规则表。
type RuleBook struct {
	rules map[string]*Rule
}

func NewRuleBook() *RuleBook {
	return &RuleBook{rules: make(map[string]*Rule)}
}

func key(proto, version string) string { return proto + "@" + version }

func (b *RuleBook) Register(r *Rule) {
	k := key(r.Protocol, r.Version)
	if _, exists := b.rules[k]; exists {
		panic("transform: duplicate rule " + k)
	}
	b.rules[k] = r
}

// Convert 归一整帧。返回读数或一条携带原因码的拒绝记录，调用方不得静默丢弃。
func (b *RuleBook) Convert(dec *protocol.DecodedFrame) ([]model.Reading, *model.RejectRecord) {
	frame := dec.Frame
	rule, ok := b.rules[key(frame.Protocol, frame.Version)]
	if !ok {
		return nil, &model.RejectRecord{
			Frame: frame, Reason: model.ReasonUnsupportedVersion,
			Detail:     fmt.Sprintf("no conversion rule for %s version %q", frame.Protocol, frame.Version),
			RejectedAt: time.Now(),
		}
	}

	ts, err := rule.parseTime(dec)
	if err != nil {
		return nil, reject(frame, model.ReasonTimestampOutOfRange, "%s", err.Error())
	}

	byKey := make(map[string]protocol.Field, len(dec.Fields))
	for _, f := range dec.Fields {
		byKey[f.Key] = f
	}

	readings := make([]model.Reading, 0, len(rule.Fields))
	for _, fr := range rule.Fields {
		f, ok := byKey[fr.SourceKey]
		if !ok {
			return nil, reject(frame, model.ReasonMissingField,
				"required metric %s (source %s) is absent", fr.Metric, fr.SourceKey)
		}
		if f.Kind != protocol.KindNumber {
			return nil, reject(frame, model.ReasonFieldType,
				"metric %s (source %s): expect numeric payload", fr.Metric, fr.SourceKey)
		}
		v := f.Number*fr.Scale + fr.Offset
		if math.IsNaN(v) || math.IsInf(v, 0) || v < fr.Min || v > fr.Max {
			return nil, reject(frame, model.ReasonValueOutOfRange,
				"metric %s normalized value %v %s out of range [%v, %v]", fr.Metric, v, fr.Unit, fr.Min, fr.Max)
		}
		readings = append(readings, model.Reading{
			DeviceID:  frame.DeviceID,
			Metric:    fr.Metric,
			Value:     v,
			Unit:      fr.Unit,
			Timestamp: ts,
			FrameID:   frame.Fingerprint(),
			Protocol:  frame.Protocol,
			Version:   frame.Version,
		})
	}
	return readings, nil
}

func reject(frame model.RawFrame, reason model.RejectReason, format string, args ...any) *model.RejectRecord {
	return &model.RejectRecord{
		Frame: frame, Reason: reason, Detail: fmt.Sprintf(format, args...), RejectedAt: time.Now(),
	}
}
