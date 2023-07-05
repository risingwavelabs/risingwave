package delivery

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type orderEvent struct {
	sink.BaseSinkRecord

	OrderId        int64  `json:"order_id"`
	RestaurantId   int64  `json:"restaurant_id"`
	OrderState     string `json:"order_state"`
	OrderTimestamp string `json:"order_timestamp"`
}

func (r *orderEvent) ToPostgresSql() string {
	return fmt.Sprintf("INSERT INTO %s (order_id, restaurant_id, order_state, order_timestamp) values ('%d', '%d', '%s', '%s')",
		"delivery_orders_source", r.OrderId, r.RestaurantId, r.OrderState, r.OrderTimestamp)
}

func (r *orderEvent) ToJson() (topic string, key string, data []byte) {
	data, _ = json.Marshal(r)
	return "delivery_orders", fmt.Sprint(r.OrderId), data
}

type orderEventGen struct {
	seqOrderId int64
	cfg        gen.GeneratorConfig
}

func NewOrderEventGen(cfg gen.GeneratorConfig) gen.LoadGenerator {
	return &orderEventGen{
		seqOrderId: 0,
		cfg:        cfg,
	}
}

func (g *orderEventGen) KafkaTopics() []string {
	return []string{"delivery_orders"}
}

func (g *orderEventGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	order_states := []string{
		"CREATED",
		"PENDING",
		"DELIVERED",
	}

	var num_of_restaurants int64 = 3
	var total_minutes = 30

	for {
		now := time.Now()
		record := &orderEvent{
			OrderId:        g.seqOrderId,
			RestaurantId:   rand.Int63n(num_of_restaurants),
			OrderState:     order_states[rand.Intn(len(order_states))],
			OrderTimestamp: now.Add(time.Duration(rand.Intn(total_minutes)) * time.Minute).Format(gen.RwTimestampLayout),
		}
		g.seqOrderId++
		select {
		case <-ctx.Done():
			return
		case outCh <- record:
		}
	}
}
