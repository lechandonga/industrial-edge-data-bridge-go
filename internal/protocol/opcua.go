package protocol

import (
	"encoding/json"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// OPCUAFrame 是模拟 OPC-UA 客户端订阅到的 JSON 报文。
// v1: ts 为 RFC3339 字符串；nodes 内为节点点名 -> {value, quality}；
//
//	temperature 节点原始值为 deci°C(0.1°C)，pressure 节点原始值为 Pa。
type OPCUAFrame struct {
	DeviceID string                     `json:"device_id"`
	TS       *json.RawMessage           `json:"ts"`
	Nodes    map[string]json.RawMessage `json:"nodes"`
}

type opcuaNode struct {
	Value   json.RawMessage `json:"value"`
	Quality *string         `json:"quality"`
}

type OPCUADecoder struct{}

func (OPCUADecoder) Protocol() string { return "opcua" }

func (OPCUADecoder) Decode(frame model.RawFrame) (*DecodedFrame, error) {
	var of OPCUAFrame
	dec := json.NewDecoder(bytesReader(frame.Raw))
	dec.UseNumber()
	if err := dec.Decode(&of); err != nil {
		return nil, reject(model.ReasonMalformedFrame, "invalid json: %v", err)
	}
	if of.DeviceID == "" {
		return nil, reject(model.ReasonMissingField, "device_id is required")
	}
	if len(of.Nodes) == 0 {
		return nil, reject(model.ReasonMissingField, "nodes must contain at least one node")
	}

	out := &DecodedFrame{Frame: frame}
	if of.TS != nil && len(*of.TS) > 0 && string(*of.TS) != "null" {
		var tv any
		d := json.NewDecoder(bytesReader(*of.TS))
		d.UseNumber()
		if err := d.Decode(&tv); err != nil {
			return nil, reject(model.ReasonFieldType, "ts is not decodable: %v", err)
		}
		s, err := asString(tv, "ts")
		if err != nil {
			return nil, err
		}
		out.HasTimestamp = true
		out.TimestampStr = s // 时间戳语义（RFC3339）由版本规则解释
	}

	for node, raw := range of.Nodes {
		var n opcuaNode
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil, reject(model.ReasonMalformedFrame, "nodes.%s: %v", node, err)
		}
		if n.Quality != nil && *n.Quality != "GOOD" {
			// 质量戳非 GOOD 属于必填语义不满足，显式拒绝而非当作正常值。
			return nil, reject(model.ReasonMissingField, "nodes.%s: quality=%s (require GOOD)", node, *n.Quality)
		}
		if len(n.Value) == 0 {
			return nil, reject(model.ReasonMissingField, "nodes.%s: value is required", node)
		}
		var v any
		d := json.NewDecoder(bytesReader(n.Value))
		d.UseNumber()
		if err := d.Decode(&v); err != nil {
			return nil, reject(model.ReasonFieldType, "nodes.%s.value: %v", node, err)
		}
		f := Field{Key: node}
		switch val := v.(type) {
		case bool:
			f.Kind, f.Bool = KindBool, val
		case string:
			f.Kind, f.Str = KindString, val
		default:
			num, err := asNumber(val, "nodes."+node+".value")
			if err != nil {
				return nil, err
			}
			f.Kind, f.Number = KindNumber, num
		}
		out.Fields = append(out.Fields, f)
	}
	return out, nil
}
