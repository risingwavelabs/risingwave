package ecommercenested

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"sync/atomic"
)

type cdnMetricsGen struct {
	cfg gen.GeneratorConfig
}

func NewCdnMetricsGen(cfg gen.GeneratorConfig) gen.LoadGenerator {
	return &cdnMetricsGen{cfg: cfg}
}

func (g *cdnMetricsGen) KafkaTopics() []string {
	return []string{"users", "orders"}
}

func (g *cdnMetricsGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	maxUserId := atomic.Pointer[int64]{}
	zero := int64(0)
	maxUserId.Store(&zero)

	go NewNestedEcommerceGen(&maxUserId).Load(ctx, outCh)
	go NewUserGen(&maxUserId).Load(ctx, outCh)
}
