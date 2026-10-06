// Package protocol 负责把不同设备协议的原始报文解码为中间结构。
// 解码阶段只校验“报文自身是否合法”，不做单位/量程等语义归一。
package protocol

import (
	"encoding/json"
	"fmt"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// FieldKind 标识解码后字段的原始类型，类型异常在此阶段即可识别。
type FieldKind int

const (
	KindNumber FieldKind = iota
	KindBool
	KindString
)

// Field 是协议无关的解码字段。Key 由各协议自行定义（寄存器地址/点名/节点 ID）。
type Field struct {
	Key    string
	Kind   FieldKind
	Number float64
	Str    string
	Bool   bool
}

// DecodedFrame 是解码后的中间结构，时间戳保留原始数值交由版本规则解释其单位。
type DecodedFrame struct {
	Frame        model.RawFrame
	Fields       []Field
	HasTimestamp bool
	TimestampRaw float64
	TimestampStr string
}

// Decoder 按协议解码原始报文。
type Decoder interface {
	Protocol() string
	Decode(frame model.RawFrame) (*DecodedFrame, error)
}

// Registry 按协议名管理解码器，未注册协议必须被显式拒绝。
type Registry struct {
	decoders map[string]Decoder
}

func NewRegistry() *Registry {
	return &Registry{decoders: make(map[string]Decoder)}
}

func (r *Registry) Register(d Decoder) {
	r.decoders[d.Protocol()] = d
}

// Decode 返回 (decoded, rejectReason, detail)。已知协议的报文问题由解码器给出原因。
func (r *Registry) Decode(frame model.RawFrame) (*DecodedFrame, model.RejectReason, string) {
	d, ok := r.decoders[frame.Protocol]
	if !ok {
		return nil, model.ReasonUnknownProtocol, fmt.Sprintf("protocol %q is not registered", frame.Protocol)
	}
	dec, err := d.Decode(frame)
	if err != nil {
		if pe, ok := err.(*FrameError); ok {
			return nil, pe.Reason, pe.Detail
		}
		return nil, model.ReasonMalformedFrame, err.Error()
	}
	return dec, "", ""
}

// FrameError 携带可区分的拒绝原因。
type FrameError struct {
	Reason model.RejectReason
	Detail string
}

func (e *FrameError) Error() string { return string(e.Reason) + ": " + e.Detail }

func reject(reason model.RejectReason, format string, args ...any) error {
	return &FrameError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// JSONNumber 辅助：严格区分字符串/布尔/数值，避免静默的隐式转换。
func asNumber(v any, where string) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, reject(model.ReasonFieldType, "%s: numeric field is not a number: %v", where, n)
		}
		return f, nil
	default:
		return 0, reject(model.ReasonFieldType, "%s: expect number, got %T", where, v)
	}
}

func asString(v any, where string) (string, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}
	return "", reject(model.ReasonFieldType, "%s: expect string, got %T", where, v)
}

func asBool(v any, where string) (bool, error) {
	if b, ok := v.(bool); ok {
		return b, nil
	}
	// 0/1 数值不自动当作布尔，协议必须明确声明，避免语义猜测。
	return false, reject(model.ReasonFieldType, "%s: expect bool, got %T", where, v)
}
