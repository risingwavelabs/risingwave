package ecommerce

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

// The order details.
type orderEvent struct {
	sink.BaseSinkRecord

	OrderId        int64   `json:"order_id"`
	ItemId         int64   `json:"item_id"`
	ItemPrice      float64 `json:"item_price"`
	EventTimestmap string  `json:"event_timestamp"`
}

func (r *orderEvent) ToPostgresSql() string {
	return fmt.Sprintf(`INSERT INTO %s
(order_id, item_id, item_price, event_timestamp)
values ('%d', '%d', %f, '%s')`,
		"order_events", r.OrderId, r.ItemId, r.ItemPrice, r.EventTimestmap)
}

func (r *orderEvent) ToJson() (topic string, key string, data []byte) {
	data, _ = json.Marshal(r)
	return "order_events", fmt.Sprint(r.OrderId), data
}

// Each order/trade will be composed of two events:
// An 'order_created' event and a 'parcel_shipped' event.
type parcelEvent struct {
	sink.BaseSinkRecord

	OrderId        int64  `json:"order_id"`
	EventTimestmap string `json:"event_timestamp"`
	EventType      string `json:"event_type"`
}

func (r *parcelEvent) ToPostgresSql() string {
	return fmt.Sprintf(`INSERT INTO %s
(order_id, event_timestamp, event_type)
values ('%d', '%s', '%s')`,
		"parcel_events", r.OrderId, r.EventTimestmap, r.EventType)
}

func (r *parcelEvent) ToJson() (topic string, key string, data []byte) {
	data, _ = json.Marshal(r)
	return "parcel_events", fmt.Sprint(r.OrderId), data
}

type ecommerceGen struct {
	faker *gofakeit.Faker

	// We simply model orders as a sliding window. `seqOrderId` advances as new orders are created.
	// `seqShipId` is always smaller than `seqOrderId` and is advanced when a new order is shipped.
	seqOrderId int64
	seqShipId  int64

	// Item ID -> Item Price
	items []float64
}

func NewEcommerceGen() gen.LoadGenerator {
	const numItems = 1000
	items := make([]float64, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = gofakeit.Float64Range(0, 10000)
	}
	return &ecommerceGen{
		faker:      gofakeit.New(0),
		seqOrderId: 0,
		seqShipId:  0,
		items:      items,
	}
}

func (g *ecommerceGen) KafkaTopics() []string {
	return []string{"order_events", "parcel_events"}
}

func (g *ecommerceGen) generate() []sink.SinkRecord {
	ts := time.Now().Format(gen.RwTimestampLayout)

	if g.faker.Bool() && g.seqShipId >= g.seqOrderId {
		// New order.
		g.seqOrderId++
		itemsNum := g.faker.IntRange(1, 4)
		orders := make([]sink.SinkRecord, itemsNum)
		for i := 0; i < itemsNum; i++ {
			itemId := rand.Intn(len(g.items))
			itemPrice := g.items[itemId]
			orders[i] = &orderEvent{
				OrderId:        g.seqOrderId,
				ItemId:         int64(itemId),
				ItemPrice:      itemPrice,
				EventTimestmap: ts,
			}
		}
		var records []sink.SinkRecord
		records = append(records, orders...)
		records = append(records, &parcelEvent{
			OrderId:        g.seqOrderId,
			EventTimestmap: ts,
			EventType:      "order_created",
		})
		return records
	} else {
		// Ship order.
		g.seqShipId++
		return []sink.SinkRecord{
			&parcelEvent{
				OrderId:        g.seqShipId,
				EventType:      "parcel_shipped",
				EventTimestmap: ts,
			},
		}
	}
}

func (g *ecommerceGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	for {
		records := g.generate()
		for _, record := range records {
			select {
			case <-ctx.Done():
				return
			case outCh <- record:
			}
		}
	}
}
