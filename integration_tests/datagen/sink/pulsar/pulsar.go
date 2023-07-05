package pulsar

import (
	"context"
	"datagen/sink"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
)

type PulsarConfig struct {
	Brokers string
}

type PulsarSink struct {
	client    pulsar.Client
	producers map[string]pulsar.Producer
}

func OpenPulsarSink(ctx context.Context, cfg PulsarConfig) (*PulsarSink, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: fmt.Sprintf("pulsar://%s", cfg.Brokers),
	})
	if err != nil {
		return nil, err
	}
	return &PulsarSink{
		client:    client,
		producers: make(map[string]pulsar.Producer),
	}, nil
}

func (p *PulsarSink) Prepare(topics []string) error {
	return nil
}

func (p *PulsarSink) Close() error {
	p.client.Close()
	return nil
}

func (p *PulsarSink) WriteRecord(ctx context.Context, format string, record sink.SinkRecord) error {
	var err error
	topic, key, data := sink.RecordToKafka(record, format)
	producer, ok := p.producers[topic]
	if !ok {
		producer, err = p.client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
		})
		if err != nil {
			return err
		}
		p.producers[topic] = producer
	}
	_, err = producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: data,
		Key:     key,
	})
	return err
}
