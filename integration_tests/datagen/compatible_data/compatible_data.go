package compatible_data

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

type Struct struct {
	S_int32 int32 `json:"s_int32"`
	S_bool  bool  `json:"s_bool"`
}

type compatibleData struct {
	sink.BaseSinkRecord

	Id                       int32     `json:"id"`
	C_boolean                bool      `json:"c_boolean"`
	C_smallint               int16     `json:"c_smallint"`
	C_integer                int32     `json:"c_integer"`
	C_bigint                 int64     `json:"c_bigint"`
	C_decimal                string    `json:"c_decimal"`
	C_real                   float32   `json:"c_real"`
	C_double_precision       float64   `json:"c_double_precision"`
	C_varchar                string    `json:"c_varchar"`
	C_bytea                  string    `json:"c_bytea"`
	C_date                   string    `json:"c_date"`
	C_time                   string    `json:"c_time"`
	C_timestamp              string    `json:"c_timestamp"`
	C_timestamptz            string    `json:"c_timestamptz"`
	C_interval               string    `json:"c_interval"`
	C_jsonb                  string    `json:"c_jsonb"`
	C_boolean_array          []bool    `json:"c_boolean_array"`
	C_smallint_array         []int16   `json:"c_smallint_array"`
	C_integer_array          []int32   `json:"c_integer_array"`
	C_bigint_array           []int64   `json:"c_bigint_array"`
	C_decimal_array          []string  `json:"c_decimal_array"`
	C_real_array             []float32 `json:"c_real_array"`
	C_double_precision_array []float64 `json:"c_double_precision_array"`
	C_varchar_array          []string  `json:"c_varchar_array"`
	C_bytea_array            []string  `json:"c_bytea_array"`
	C_date_array             []string  `json:"c_date_array"`
	C_time_array             []string  `json:"c_time_array"`
	C_timestamp_array        []string  `json:"c_timestamp_array"`
	C_timestamptz_array      []string  `json:"c_timestamptz_array"`
	C_interval_array         []string  `json:"c_interval_array"`
	C_jsonb_array            []string  `json:"c_jsonb_array"`
	C_struct                 Struct    `json:"c_struct"`
}

func (c *compatibleData) Topic() string {
	return "compatible_data"
}

func (c *compatibleData) Key() string {
	return fmt.Sprintf("%d", c.Id)
}

func (c *compatibleData) ToPostgresSql() string {
	panic("unimplemented")
}

func (c *compatibleData) ToJson() []byte {
	data, err := json.Marshal(c)
	if err != nil {
		panic("failed to marshal compatible data to JSON")
	}
	return data
}

func (c *compatibleData) ToProtobuf() []byte {
	panic("unimplemented")
}

func (c *compatibleData) ToAvro() []byte {
	panic("unimplemented")
}

type compatibleDataGen struct {
	recordSum int32
}

func NewCompatibleDataGen() gen.LoadGenerator {
	return &compatibleDataGen{}
}

func (g *compatibleDataGen) GenData() compatibleData {
	g.recordSum++
	recordType := g.recordSum % 3
	if recordType == 0 {
		return compatibleData{
			Id:                 g.recordSum,
			C_boolean:          true,
			C_smallint:         0,
			C_integer:          0,
			C_bigint:           0,
			C_decimal:          "nan",
			C_real:             0,
			C_double_precision: 0,
			C_varchar:          "",
			C_bytea:            "",
			C_date:             "0001-01-01",
			C_time:             "00:00:00",
			C_timestamp:        "0001-01-01 00:00:00",
			C_timestamptz:      "0001-01-01 00:00:00Z",
			C_interval:         "P0Y0M0DT0H0M0S",
			C_jsonb:            "{}",
		}
	} else if recordType == 1 {
		return compatibleData{
			Id:                       g.recordSum,
			C_boolean:                false,
			C_smallint:               math.MinInt16,
			C_integer:                math.MinInt32,
			C_bigint:                 math.MinInt64,
			C_decimal:                "-123456789.123456789",
			C_real:                   -9999.999999,
			C_double_precision:       -10000.0,
			C_varchar:                "a",
			C_bytea:                  "a",
			C_date:                   "1970-01-01",
			C_time:                   "00:00:00.123456",
			C_timestamp:              "1970-01-01 00:00:00.123456",
			C_timestamptz:            "1970-01-01 00:00:00.123456Z",
			C_interval:               "P1Y2M3DT4H5M6S",
			C_jsonb:                  "{}",
			C_boolean_array:          []bool{true, false},
			C_smallint_array:         []int16{1},
			C_integer_array:          []int32{1},
			C_bigint_array:           []int64{1},
			C_decimal_array:          []string{"1.0"},
			C_real_array:             []float32{1.0},
			C_double_precision_array: []float64{1.0},
			C_varchar_array:          []string{"aa"},
			C_bytea_array:            []string{"aa"},
			C_date_array:             []string{"1970-01-01"},
			C_time_array:             []string{"00:00:00.123456"},
			C_timestamp_array:        []string{"1970-01-01 00:00:00.123456"},
			C_timestamptz_array:      []string{"1970-01-01 00:00:00.123456Z"},
			C_interval_array:         []string{"P0Y0M0DT0H0M2S"},
			C_jsonb_array:            []string{"{}"},
			C_struct:                 Struct{1, true},
		}
	} else {
		return compatibleData{
			Id:                       g.recordSum,
			C_boolean:                true,
			C_smallint:               math.MaxInt16,
			C_integer:                math.MaxInt32,
			C_bigint:                 math.MaxInt64,
			C_decimal:                "123456789.123456789",
			C_real:                   9999.999999,
			C_double_precision:       10000.0,
			C_varchar:                strings.Repeat("a", 100),
			C_bytea:                  strings.Repeat("b", 100),
			C_date:                   "9999-12-31",
			C_time:                   "23:59:59.999999",
			C_timestamp:              "9999-12-31 23:59:59.999999",
			C_timestamptz:            "9999-12-31 23:59:59.999999Z",
			C_interval:               "P1Y2M3DT4H5M6S",
			C_jsonb:                  "{\"mean\":1}",
			C_boolean_array:          []bool{true, false},
			C_smallint_array:         []int16{1},
			C_integer_array:          []int32{1},
			C_bigint_array:           []int64{1},
			C_decimal_array:          []string{"1.0"},
			C_real_array:             []float32{1.0},
			C_double_precision_array: []float64{1.0},
			C_varchar_array:          []string{"aa"},
			C_bytea_array:            []string{"aa"},
			C_date_array:             []string{"1970-01-01"},
			C_time_array:             []string{"00:00:00.123456"},
			C_timestamp_array:        []string{"1970-01-01 00:00:00.123456"},
			C_timestamptz_array:      []string{"1970-01-01 00:00:00.123456Z"},
			C_interval_array:         []string{"P1Y2M3DT4H5M6S"},
			C_jsonb_array:            []string{"{}"},
			C_struct:                 Struct{-1, false},
		}
	}
}

func (g *compatibleDataGen) KafkaTopics() []string {
	return []string{"compatible_data"}
}

func (g *compatibleDataGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	for {
		record := g.GenData()
		select {
		case <-ctx.Done():
			return
		case outCh <- &record:
		}
	}
}
