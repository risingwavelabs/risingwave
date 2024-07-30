package nats

import (
	"context"
	"datagen/sink"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	_ "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NatsConfig struct {
	Url       string
	JetStream bool
}

type NatsSink struct {
	nc     *nats.Conn
	config NatsConfig
	js     jetstream.JetStream
}

func OpenNatsSink(config NatsConfig) (*NatsSink, error) {
	nc, err := nats.Connect(config.Url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream instance: %v", err)
	}
	return &NatsSink{nc, config, js}, nil
}

func (p *NatsSink) Prepare(topics []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if p.config.JetStream {
		_, err := p.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     "risingwave",
			Subjects: topics,
		})
		if err != nil {
			return fmt.Errorf("failed to create JetStream stream: %v", err)
		}
	}
	return nil
}

func (p *NatsSink) Close() error {
	p.nc.Close()
	return nil
}

func (p *NatsSink) WriteRecord(ctx context.Context, format string, record sink.SinkRecord) error {
	data := sink.Encode(record, format)

	if p.config.JetStream {
		_, err := p.js.Publish(ctx, record.Topic(), data)
		if err != nil {
			return fmt.Errorf("failed to publish record to JetStream: %v", err)
		}
	} else {
		err := p.nc.Publish(record.Topic(), data)
		if err != nil {
			return fmt.Errorf("failed to request NATS server: %v", err)
		}
	}
	return nil
}

func (p *NatsSink) Flush(ctx context.Context) error {
	return p.nc.Flush()
}
