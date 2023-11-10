package ecommercenested

import (
	"context"
	"datagen/gen"
	"datagen/sink"
)

type cdnMetricsGen struct {
	cfg gen.GeneratorConfig
}

func NewCdnMetricsGen(cfg gen.GeneratorConfig) gen.LoadGenerator {
	return &cdnMetricsGen{cfg: cfg}
}

func (g *cdnMetricsGen) KafkaTopics() []string {
	return []string{"tcp_metrics", "nics_metrics"}
}

func (g *cdnMetricsGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	go func() {
		g := NewNestedEcommerceGen()
		g.Load(ctx, outCh)
	}()
	go func() {
		g := NewUserGen()
		g.Load(ctx, outCh)
	}()
}
