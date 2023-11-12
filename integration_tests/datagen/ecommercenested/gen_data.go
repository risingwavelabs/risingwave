package ecommercenested

import (
	"context"
	"datagen/sink"
	"sync/atomic"
	"time"
)

type nestedGen struct{}

func Newgen() *nestedGen {
	return &nestedGen{}
}

func (g *nestedGen) KafkaTopics() []string {
	pe := parcelEvent{}
	oe := orderEvent{}
	ue := userEvent{}
	return []string{pe.Topic(), oe.Topic(), ue.Topic()}
}

// TODO: how does it know to which topics to send?
func (g *nestedGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {

	time.Sleep(10 * time.Second) // TODO: remove.

	maxUserId := atomic.Pointer[int64]{}
	zero := int64(0)
	maxUserId.Store(&zero)

	go NewNestedEcommerceGen(&maxUserId).Load(ctx, outCh)
	go NewUserGen(&maxUserId).Load(ctx, outCh)
}
