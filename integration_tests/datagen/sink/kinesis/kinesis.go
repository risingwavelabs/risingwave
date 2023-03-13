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
}

type KinesisSink struct {
	client *kinesis.Kinesis
	cfg    KinesisConfig
}

func OpenKinesisSink(cfg KinesisConfig) (*KinesisSink, error) {
	ss := session.Must(session.NewSession())
	client := kinesis.New(ss, aws.NewConfig().WithRegion(cfg.Region))
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
	_, key, data := sink.RecordToKafka(record, format)
	_, err := p.client.PutRecordWithContext(ctx, &kinesis.PutRecordInput{
		Data:         data,
		PartitionKey: aws.String(key),
		StreamName:   aws.String(p.cfg.StreamName),
	})
	if err != nil {
		return fmt.Errorf("failed to write record to kinesis: %s", err)
	} else {
		return nil
	}
}
