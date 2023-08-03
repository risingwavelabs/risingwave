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
	Bucket   string
	Region   string
	Endpoint string
}

type S3Sink struct {
	buffer bytes.Buffer
	client *s3.S3
	cfg    S3Config
}

func OpenS3Sink(cfg S3Config) (*S3Sink, error) {
	ss := session.Must(session.NewSession())
	config := aws.NewConfig().WithRegion(cfg.Region)
	if cfg.Endpoint != "" {
		config = config.WithEndpoint(cfg.Endpoint).WithS3ForcePathStyle(true)
	}
	client := s3.New(ss, config)
	return &S3Sink{
		client: client,
		cfg:    cfg,
	}, nil
}

func (p *S3Sink) Prepare(topics []string) error {
	return nil
}

func (p *S3Sink) Close() error {
	return p.Flush(context.Background())
}

func (p *S3Sink) WriteRecord(ctx context.Context, format string, record sink.SinkRecord) error {
	data := sink.Encode(record, format)

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

func (p *S3Sink) Flush(ctx context.Context) error {
	name := fmt.Sprintf("data-%d", time.Now().UnixMilli())

	_, err := p.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(name),
		Body:   bytes.NewReader(p.buffer.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("failed to put object to s3: %s", err)
	}

	fmt.Printf("S3 Sink: uploaded object '%s' (%d bytes)\n", name, p.buffer.Len())

	p.buffer.Reset()
	return nil
}
