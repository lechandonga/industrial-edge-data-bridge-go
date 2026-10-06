package protocol

import (
	"encoding/json"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// ModbusFrame 是模拟 Modbus 网关上报的 JSON 报文。
// v1: ts 单位为秒，寄存器 40001=温度(0.1°C 缩放), 40002=压力(kPa)
// v2: ts 单位为毫秒，寄存器 40001=温度(°C 原始值), 40002=压力(MPa)
// 寄存器值类型异常、时间戳字段异常都在解码阶段拒绝。
type ModbusFrame struct {
	DeviceID string                     `json:"device_id"`
	TS       *json.RawMessage           `json:"ts"`
	Regs     map[string]json.RawMessage `json:"regs"`
}

type ModbusDecoder struct{}

func (ModbusDecoder) Protocol() string { return "modbus" }

func (ModbusDecoder) Decode(frame model.RawFrame) (*DecodedFrame, error) {
	var mf ModbusFrame
	dec := json.NewDecoder(bytesReader(frame.Raw))
	dec.UseNumber()
	if err := dec.Decode(&mf); err != nil {
		return nil, reject(model.ReasonMalformedFrame, "invalid json: %v", err)
	}
	if mf.DeviceID == "" {
		return nil, reject(model.ReasonMissingField, "device_id is required")
	}
	if len(mf.Regs) == 0 {
		return nil, reject(model.ReasonMissingField, "regs must contain at least one register")
	}

	out := &DecodedFrame{Frame: frame}
	if mf.TS != nil && len(*mf.TS) > 0 && string(*mf.TS) != "null" {
		var tv any
		d := json.NewDecoder(bytesReader(*mf.TS))
		d.UseNumber()
		if err := d.Decode(&tv); err != nil {
			return nil, reject(model.ReasonFieldType, "ts is not decodable: %v", err)
		}
		n, err := asNumber(tv, "ts")
		if err != nil {
			return nil, err
		}
		out.HasTimestamp = true
		out.TimestampRaw = n
	}

	for addr, raw := range mf.Regs {
		var v any
		d := json.NewDecoder(bytesReader(raw))
		d.UseNumber()
		if err := d.Decode(&v); err != nil {
			return nil, reject(model.ReasonFieldType, "regs.%s: %v", addr, err)
		}
		f := Field{Key: addr}
		switch val := v.(type) {
		case bool:
			f.Kind, f.Bool = KindBool, val
		case string:
			f.Kind, f.Str = KindString, val
		default:
			n, err := asNumber(val, "regs."+addr)
			if err != nil {
				return nil, err
			}
			f.Kind, f.Number = KindNumber, n
		}
		out.Fields = append(out.Fields, f)
	}
	return out, nil
}
