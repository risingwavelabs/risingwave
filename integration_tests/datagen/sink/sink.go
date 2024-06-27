package sink

import (
	"context"
)

type SinkRecord interface {
	// Topic that the record belongs to.
	Topic() string

	// Record key for partitioning.
	Key() string

	// Convert the event to an INSERT INTO command.
	ToPostgresSql() string

	// Convert the event to a Kafka message in JSON format.
	// This interface will also be used for Pulsar and Kinesis.
	ToJson() []byte

	// Convert the event to a Kafka message in Protobuf format.
	// This interface will also be used for Pulsar and Kinesis.
	ToProtobuf() []byte

	// Convert the event to a Kafka message in Avro format.
	// This interface will also be used for Pulsar and Kinesis.
	ToAvro() []byte
}

type BaseSinkRecord struct {
}

func (r BaseSinkRecord) Topic() string {
	panic("not implemented")
}

func (r BaseSinkRecord) Key() string {
	panic("not implemented")
}

func (r BaseSinkRecord) ToPostgresSql() string {
	panic("not implemented")
}

func (r BaseSinkRecord) ToJson() []byte {
	panic("not implemented")
}

func (r BaseSinkRecord) ToProtobuf() []byte {
	panic("not implemented")
}

func (r BaseSinkRecord) ToAvro() []byte {
	panic("not implemented")
}

// Convert the event to a message in the given format.
func Encode(r SinkRecord, format string) []byte {
	if format == "json" {
		return r.ToJson()
	} else if format == "protobuf" {
		return r.ToProtobuf()
	} else if format == "avro" {
		return r.ToAvro()
	} else {
		panic("unsupported format")
	}
}

type Sink interface {
	Prepare(topics []string) error

	WriteRecord(ctx context.Context, format string, record SinkRecord) error

	Flush(ctx context.Context) error

	Close() error
}
