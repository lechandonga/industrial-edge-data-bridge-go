package protocol

import (
	"fmt"
	"testing"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

func payload(batchID, version string, ts int64, fields string) []byte {
	return []byte(fmt.Sprintf(`{"batch_id":%q,"device_id":"dev-1","version":%q,"ts":%d,"fields":%s}`,
		batchID, version, ts, fields))
}

func pointByMetric(t *testing.T, b *model.Batch, metric string) model.Point {
	t.Helper()
	for _, p := range b.Points {
		if p.Metric == metric {
			return p
		}
	}
	t.Fatalf("metric %s not found in batch", metric)
	return model.Point{}
}

func TestNormalizeModsimV1Scaling(t *testing.T) {
	t.Log("业务场景: 版本语义转换 - modsim v1 原始整型按缩放系数归一")
	reg := DefaultRegistry()
	b, err := reg.ParseAndNormalize("modsim", payload("b1", "v1", time.Now().UnixMilli(), `{"temp":255,"pressure":5000}`))
	if err != nil {
		t.Fatalf("unexpected reject: %v", err)
	}
	temp := pointByMetric(t, b, "temperature")
	if got := temp.Value; got != 25.5 {
		t.Fatalf("temp: got %v want 25.5", got)
	}
	if temp.Unit != "celsius" {
		t.Fatalf("unit: got %s", temp.Unit)
	}
	t.Logf("用例通过: temp 255 -> %v celsius, pressure 5000 -> %v kpa",
		temp.Value, pointByMetric(t, b, "pressure").Value)
}

func TestNormalizeModsimV2UnitConversion(t *testing.T) {
	t.Log("业务场景: 版本语义转换 - modsim v2 华氏度/psi 归一到摄氏度/kPa")
	reg := DefaultRegistry()
	b, err := reg.ParseAndNormalize("modsim", payload("b2", "v2", time.Now().UnixMilli(), `{"temp":212,"pressure":10}`))
	if err != nil {
		t.Fatalf("unexpected reject: %v", err)
	}
	temp := pointByMetric(t, b, "temperature")
	pressure := pointByMetric(t, b, "pressure")
	if got := temp.Value; got < 99.99 || got > 100.01 {
		t.Fatalf("212F should be 100C, got %v", got)
	}
	if got := pressure.Value; got < 68.9 || got > 69.0 {
		t.Fatalf("10psi should be ~68.95kpa, got %v", got)
	}
	t.Logf("用例通过: 212F -> %.2fC, 10psi -> %.2fkpa", temp.Value, pressure.Value)
}

func TestRejectUnknownVersion(t *testing.T) {
	t.Log("业务场景: 版本不兼容 - 未知版本必须明确拒绝而非猜测")
	reg := DefaultRegistry()
	_, err := reg.ParseAndNormalize("modsim", payload("b3", "v99", time.Now().UnixMilli(), `{"temp":1,"pressure":1}`))
	re, ok := model.IsReject(err)
	if !ok || re.Reason != model.ReasonUnknownVersion {
		t.Fatalf("want unknown_version reject, got %v", err)
	}
	if re.BatchID != "b3" {
		t.Fatalf("reject must be traceable to batch b3, got %q", re.BatchID)
	}
	t.Logf("用例通过: 拒绝原因=%s, 可追溯批次=%s", re.Reason, re.BatchID)
}

func TestRejectBadFieldType(t *testing.T) {
	t.Log("业务场景: 字段类型异常 - 字符串温度值必须拒绝并给出原因")
	reg := DefaultRegistry()
	_, err := reg.ParseAndNormalize("modsim", payload("b4", "v1", time.Now().UnixMilli(), `{"temp":"hot","pressure":1}`))
	re, ok := model.IsReject(err)
	if !ok || re.Reason != model.ReasonBadFieldType || re.Field != "temp" {
		t.Fatalf("want bad_field_type on temp, got %v", err)
	}
	t.Logf("用例通过: 拒绝原因=%s 字段=%s", re.Reason, re.Field)
}

func TestRejectMissingField(t *testing.T) {
	t.Log("业务场景: 必填字段缺失 - 缺少 pressure 必须拒绝")
	reg := DefaultRegistry()
	_, err := reg.ParseAndNormalize("modsim", payload("b5", "v1", time.Now().UnixMilli(), `{"temp":100}`))
	re, ok := model.IsReject(err)
	if !ok || re.Reason != model.ReasonMissingField || re.Field != "pressure" {
		t.Fatalf("want missing_field pressure, got %v", err)
	}
	t.Logf("用例通过: 拒绝原因=%s 字段=%s", re.Reason, re.Field)
}

func TestRejectTimestampOutOfRange(t *testing.T) {
	t.Log("业务场景: 时间戳越界 - 1970 年的时间戳必须拒绝")
	reg := DefaultRegistry()
	_, err := reg.ParseAndNormalize("modsim", payload("b6", "v1", 1000, `{"temp":100,"pressure":1}`))
	re, ok := model.IsReject(err)
	if !ok || re.Reason != model.ReasonTimestampOutOfRange {
		t.Fatalf("want timestamp_out_of_range, got %v", err)
	}
	t.Logf("用例通过: 拒绝原因=%s", re.Reason)
}

func TestRejectValueOutOfRange(t *testing.T) {
	t.Log("业务场景: 量程越界 - 归一化后超量程必须拒绝")
	reg := DefaultRegistry()
	_, err := reg.ParseAndNormalize("modsim", payload("b7", "v1", time.Now().UnixMilli(), `{"temp":99999,"pressure":1}`))
	re, ok := model.IsReject(err)
	if !ok || re.Reason != model.ReasonValueOutOfRange {
		t.Fatalf("want value_out_of_range, got %v", err)
	}
	t.Logf("用例通过: 拒绝原因=%s", re.Reason)
}
