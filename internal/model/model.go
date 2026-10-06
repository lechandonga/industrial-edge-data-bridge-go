package model

import (
	"fmt"
	"time"
)

// RejectReason 是记录被拒绝时的可区分原因码，拒绝不会静默发生。
type RejectReason string

const (
	// ReasonUnknownProtocol 设备上报了注册表中不存在的协议。
	ReasonUnknownProtocol RejectReason = "UNKNOWN_PROTOCOL"
	// ReasonUnsupportedVersion 协议已知，但版本没有对应的转换规则。
	ReasonUnsupportedVersion RejectReason = "UNSUPPORTED_VERSION"
	// ReasonMalformedFrame 原始报文无法按协议解码（如非法 JSON / 载荷缺失）。
	ReasonMalformedFrame RejectReason = "MALFORMED_FRAME"
	// ReasonMissingField 必填字段缺失。
	ReasonMissingField RejectReason = "MISSING_FIELD"
	// ReasonFieldType 字段类型与协议声明不符，无法安全转换。
	ReasonFieldType RejectReason = "FIELD_TYPE"
	// ReasonTimestampOutOfRange 时间戳缺失精度或落在允许窗口之外。
	ReasonTimestampOutOfRange RejectReason = "TIMESTAMP_OUT_OF_RANGE"
	// ReasonValueOutOfRange 归一化后的数值超出该测点允许量程。
	ReasonValueOutOfRange RejectReason = "VALUE_OUT_OF_RANGE"
	// ReasonDuplicateFrame 同一设备的同一报文序号被重复上报。
	ReasonDuplicateFrame RejectReason = "DUPLICATE_FRAME"
	// ReasonBufferOverflow 缓冲区达到上限且策略为拒绝背压（调用方可见的错误，不丢弃）。
	ReasonBufferOverflow RejectReason = "BUFFER_OVERFLOW"
)

// RawFrame 是设备上报的原始报文，任何归一化后的记录都可追溯回它。
type RawFrame struct {
	Protocol string `json:"protocol"`
	Version  string `json:"version"`
	DeviceID string `json:"device_id"`
	// Seq 是设备内单调递增的报文序号，与 DeviceID 共同构成幂等键。
	Seq        uint64    `json:"seq"`
	ReceivedAt time.Time `json:"received_at"`
	// Raw 保留设备线上的原始字节，用于审计与问题追溯。
	Raw []byte `json:"raw"`
}

// Fingerprint 返回报文的幂等指纹：同设备同序号即同一报文。
func (f RawFrame) Fingerprint() string {
	return fmt.Sprintf("%s:%d", f.DeviceID, f.Seq)
}

// Reading 是跨协议归一后的统一数据模型。
type Reading struct {
	DeviceID  string    `json:"device_id"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
	Unit      string    `json:"unit"`
	Timestamp time.Time `json:"timestamp"`
	FrameID   string    `json:"frame_id"`
	Protocol  string    `json:"protocol"`
	Version   string    `json:"version"`
}

// RejectRecord 记录一条被拒绝的原始报文及可区分的原因，保证不静默丢弃。
type RejectRecord struct {
	Frame      RawFrame     `json:"frame"`
	Reason     RejectReason `json:"reason"`
	Detail     string       `json:"detail"`
	RejectedAt time.Time    `json:"rejected_at"`
}

func (r RejectRecord) Error() string {
	return fmt.Sprintf("reject frame %s/%s seq=%d: %s: %s",
		r.Frame.Protocol, r.Frame.DeviceID, r.Frame.Seq, r.Reason, r.Detail)
}
