package main

import (
	"bytes"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	gen "github.com/my_account/my_udf/gen"
)

type MyUdf struct {
}

func init() {
	a := MyUdf{}
	gen.SetUdf(a)
}

func (u MyUdf) InputSchema() gen.RisingwaveUdfTypesSchema {
	return gen.RisingwaveUdfTypesSchema{}
}
func (u MyUdf) OutputSchema() gen.RisingwaveUdfTypesSchema {
	return gen.RisingwaveUdfTypesSchema{}
}
func (u MyUdf) Eval(batch []uint8) gen.Result[[]uint8, gen.RisingwaveUdfTypesEvalErrno] {
	reader, err := ipc.NewReader(bytes.NewReader(batch))
	if err != nil {
		panic(err)
	}
	builder := array.NewBooleanBuilder(memory.NewGoAllocator())
	for reader.Next() {
		rec := reader.Record()
		col := rec.Column(0).(*array.Int64).Int64Values()

		for i := 0; i < int(rec.NumRows()); i++ {
			builder.Append(col[i] > 0)
		}
	}
	arr := builder.NewArray()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "result", Type: &arrow.BooleanType{}},
		},
		nil,
	)
	record := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))

	buffer := new(bytes.Buffer)
	writer := ipc.NewWriter(buffer, ipc.WithSchema(record.Schema()))
	writer.Write(record)

	return gen.Result[[]uint8, gen.RisingwaveUdfTypesEvalErrno]{
		Kind: gen.Ok,
		Val:  buffer.Bytes(),
		Err:  gen.RisingwaveUdfTypesEvalErrno{},
	}
}

//go:generate wit-bindgen tiny-go ../../wit --out-dir=gen
func main() {
	// Just for testing

	builder := array.NewInt64Builder(memory.NewGoAllocator())
	builder.Append(-1)
	builder.Append(0)
	builder.Append(1)
	arr := builder.NewArray()
	record := array.NewRecord(
		arrow.NewSchema(
			[]arrow.Field{
				{Name: "input", Type: &arrow.Int64Type{}},
			},
			nil,
		),
		[]arrow.Array{arr},
		int64(arr.Len()),
	)

	buffer := new(bytes.Buffer)
	writer := ipc.NewWriter(buffer, ipc.WithSchema(record.Schema()))
	writer.Write(record)

	udf := MyUdf{}

	result := udf.Eval(buffer.Bytes())
	println("result:", result.Kind)
}
