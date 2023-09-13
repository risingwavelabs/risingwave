package kinesis

import (
	"context"
	"datagen/sink"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type KinesisConfig struct {
	StreamName string
	Region     string
	Endpoint   string
}

type KinesisSink struct {
	client *kinesis.Kinesis
	cfg    KinesisConfig
}

func OpenKinesisSink(cfg KinesisConfig) (*KinesisSink, error) {
	ss := session.Must(session.NewSession())
	config := aws.NewConfig().WithRegion(cfg.Region)
	if cfg.Endpoint != "" {
		config = config.WithEndpoint(cfg.Endpoint)
	}
	client := kinesis.New(ss, config)
	return &KinesisSink{
		client: client,
		cfg:    cfg,
	}, nil
}

func (p *KinesisSink) Prepare(topics []string) error {
	return nil
}

func (p *KinesisSink) Close() error {
	return nil
}

func (p *KinesisSink) WriteRecord(ctx context.Context, format string, record sink.SinkRecord) error {
	data := sink.Encode(record, format)
	_, err := p.client.PutRecordWithContext(ctx, &kinesis.PutRecordInput{
		Data:         data,
		PartitionKey: aws.String(record.Key()),
		StreamName:   aws.String(p.cfg.StreamName),
	})
	if err != nil {
		return fmt.Errorf("failed to write record to kinesis: %s", err)
	} else {
		return nil
	}
}

func (p *KinesisSink) Flush(ctx context.Context) error {
	return nil
}
