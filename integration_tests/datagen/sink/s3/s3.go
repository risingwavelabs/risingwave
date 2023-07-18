package s3

import (
	"bytes"
	"context"
	"datagen/sink"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Config struct {
	Bucket string
	Region string
}

type S3Sink struct {
	buffer *bytes.Buffer
	client *s3.S3
	cfg    S3Config
}

func OpenS3Sink(cfg S3Config) (*S3Sink, error) {
	ss := session.Must(session.NewSession())
	client := s3.New(ss, aws.NewConfig().WithRegion(cfg.Region))
	return &S3Sink{
		client: client,
		cfg:    cfg,
	}, nil
}

func (p *S3Sink) Prepare(topics []string) error {
	return nil
}

func (p *S3Sink) Close() error {
	return p.Flush()
}

func (p *S3Sink) WriteRecord(ctx context.Context, format string, record sink.SinkRecord) error {
	topic, _, data := sink.RecordToKafka(record, format)
	fmt.Printf("%s\n", data)
	if topic == "ad_click" {
		return nil
	}

	_, err := p.buffer.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write record to buffer: %s", err)
	}

	err = p.buffer.WriteByte('\n')
	if err != nil {
		return fmt.Errorf("failed to write new-line to buffer: %s", err)
	}

	return nil
}

func (p *S3Sink) Flush() error {
	// FIXME: hard-coded JSON format
	name := fmt.Sprintf("data-%d.ndjson", time.Now().UnixMilli())

	_, err := p.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(name),
		Body:   bytes.NewReader(p.buffer.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("failed to put object to s3: %s", err)
	} else {
		return nil
	}
}
