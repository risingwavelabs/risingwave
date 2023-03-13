package main

import (
	"context"
	"datagen/ad_click"
	"datagen/ad_ctr"
	"datagen/cdn_metrics"
	"datagen/clickstream"
	"datagen/delivery"
	"datagen/ecommerce"
	"datagen/gen"
	"datagen/livestream"
	"datagen/nexmark"
	"datagen/sink"
	"datagen/sink/kafka"
	"datagen/sink/kinesis"
	"datagen/sink/mysql"
	"datagen/sink/postgres"
	"datagen/sink/pulsar"
	"datagen/twitter"
	"fmt"
	"log"
	"time"

	"go.uber.org/ratelimit"
)

func createSink(ctx context.Context, cfg gen.GeneratorConfig) (sink.Sink, error) {
	if cfg.Sink == "postgres" {
		return postgres.OpenPostgresSink(cfg.Postgres)
	} else if cfg.Sink == "mysql" {
		return mysql.OpenMysqlSink(cfg.Mysql)
	} else if cfg.Sink == "kafka" {
		return kafka.OpenKafkaSink(ctx, cfg.Kafka)
	} else if cfg.Sink == "pulsar" {
		return pulsar.OpenPulsarSink(ctx, cfg.Pulsar)
	} else if cfg.Sink == "kinesis" {
		return kinesis.OpenKinesisSink(cfg.Kinesis)
	} else {
		return nil, fmt.Errorf("invalid sink type: %s", cfg.Sink)
	}
}

// newgen creates a new generator based on the given config.
func newGen(cfg gen.GeneratorConfig) (gen.LoadGenerator, error) {
	if cfg.Mode == "ad-click" {
		return ad_click.NewAdClickGen(), nil
	} else if cfg.Mode == "ad-ctr" {
		return ad_ctr.NewAdCtrGen(), nil
	} else if cfg.Mode == "twitter" {
		return twitter.NewTwitterGen(), nil
	} else if cfg.Mode == "cdn-metrics" {
		return cdn_metrics.NewCdnMetricsGen(cfg), nil
	} else if cfg.Mode == "clickstream" {
		return clickstream.NewClickStreamGen(), nil
	} else if cfg.Mode == "ecommerce" {
		return ecommerce.NewEcommerceGen(), nil
	} else if cfg.Mode == "delivery" {
		return delivery.NewOrderEventGen(cfg), nil
	} else if cfg.Mode == "livestream" || cfg.Mode == "superset" {
		return livestream.NewLiveStreamMetricsGen(cfg), nil
	} else if cfg.Mode == "nexmark" {
		return nexmark.NewNexmarkGen(cfg), nil
	} else {
		return nil, fmt.Errorf("invalid mode: %s", cfg.Mode)
	}
}

// spawnGen spawns one or more goroutines to generate data and send it to outCh.
func spawnGen(ctx context.Context, cfg gen.GeneratorConfig, outCh chan<- sink.SinkRecord) (gen.LoadGenerator, error) {
	gen, err := newGen(cfg)
	if err != nil {
		return nil, err
	}
	go gen.Load(ctx, outCh)
	return gen, nil
}

// generateLoad generates data and sends it to the given sink.
func generateLoad(ctx context.Context, cfg gen.GeneratorConfig) error {
	sinkImpl, err := createSink(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err = sinkImpl.Close(); err != nil {
			log.Print(err)
		}
	}()

	outCh := make(chan sink.SinkRecord, 1000)
	gen, err := spawnGen(ctx, cfg, outCh)
	if err != nil {
		return err
	}

	err = sinkImpl.Prepare(gen.KafkaTopics())
	if err != nil {
		return err
	}

	count := int64(0)
	initTime := time.Now()
	prevTime := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	rl := ratelimit.New(cfg.Qps, ratelimit.WithoutSlack) // per second
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if time.Since(prevTime) >= 10*time.Second {
				log.Printf("Sent %d records in total (Elasped: %s)", count, time.Since(initTime).String())
				prevTime = time.Now()
			}
		case record := <-outCh:
			if cfg.PrintInsert {
				fmt.Println(record.ToPostgresSql())
			}
			// Consume records from the channel and send to sink.
			if err := sinkImpl.WriteRecord(ctx, cfg.Format, record); err != nil {
				return err
			}
			_ = rl.Take()
			count++
			if time.Since(prevTime) >= 10*time.Second {
				log.Printf("Sent %d records in total (Elasped: %s)", count, time.Since(initTime).String())
				prevTime = time.Now()
			}
		}
	}
}
