package ecommercenested

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type address struct {
	Town    string `json:"town"`
	ZipCode int64  `json:"zip_code"`
	Country string `json:"country"`
}

func getAddress(faker *gofakeit.Faker) address {
	return address{
		Town:    faker.City(),
		ZipCode: int64(faker.IntRange(10000, 99999)),
		Country: faker.Country(),
	}
}

type buyer struct {
	Address address `json:"address"`
	Id      uint    `json:"id"`
}

// TODO: Do I need to_json for buyer and address?

// The order details.
type orderEvent struct {
	sink.BaseSinkRecord

	OrderId        int64   `json:"order_id"`
	ItemId         int64   `json:"item_id"`
	ItemPrice      float64 `json:"item_price"`
	EventTimestamp string  `json:"event_timestamp"`
	User           buyer   `json:"buyer"`
}

func (r *orderEvent) ToPostgresSql() string {
	return fmt.Sprintf(`INSERT INTO %s
(order_id, item_id, item_price, event_timestamp)
values ('%d', '%d', %f, '%s')`,
		"order_events", r.OrderId, r.ItemId, r.ItemPrice, r.EventTimestamp)
}

func (r *orderEvent) ToJson() []byte {
	data, _ := json.Marshal(r)
	return data
}

func (r *orderEvent) Topic() string {
	return "order_events"
}

func (r *orderEvent) Key() string {
	return fmt.Sprintf("%v", r.OrderId)
}

// Each order/trade will be composed of two events:
// An 'order_created' event and a 'parcel_shipped' event.
type parcelEvent struct {
	sink.BaseSinkRecord

	OrderId        int64  `json:"order_id"`
	EventTimestamp string `json:"event_timestamp"`
	EventType      string `json:"event_type"`
}

// TODO: parcel events are way to fast
// how do we limit the number of parcel events?
// How does qps work?

func (r *parcelEvent) ToPostgresSql() string {
	return fmt.Sprintf(`INSERT INTO %s
(order_id, event_timestamp, event_type)
values ('%d', '%s', '%s')`,
		"parcel_events", r.OrderId, r.EventTimestamp, r.EventType)
}

func (r *parcelEvent) ToJson() []byte {
	data, _ := json.Marshal(r)
	return data
}

func (r *parcelEvent) Topic() string {
	return "parcel_events"
}

func (r *parcelEvent) Key() string {
	return fmt.Sprintf("%v", r.OrderId)
}

type ecommerceGen struct {
	faker *gofakeit.Faker

	// We simply model orders as a sliding window. `seqOrderId` advances as new orders are created.
	// `seqShipId` is always smaller than `seqOrderId` and is advanced when a new order is shipped.
	seqOrderId int64
	seqShipId  int64

	// Item ID -> Item Price
	items []float64

	maxUserID *atomic.Pointer[int64]
}

func NewNestedEcommerceGen(maxId *atomic.Pointer[int64]) gen.LoadGenerator {
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
		maxUserID:  maxId,
	}
}

func (g *ecommerceGen) KafkaTopics() []string {
	return []string{"order_events", "parcel_events"}
}

func (g *ecommerceGen) generate() []sink.SinkRecord {

	for *g.maxUserID.Load() < 5 {
		time.Sleep(time.Second / 10)
	}

	ts := time.Now().Format(gen.RwTimestampNaiveLayout)

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
				EventTimestamp: ts,
				User: buyer{
					Address: getAddress(g.faker),
					Id:      uint(g.faker.IntRange(0, int(*g.maxUserID.Load()))),
				},
			}
		}
		var records []sink.SinkRecord
		records = append(records, orders...)
		records = append(records, &parcelEvent{
			OrderId:        g.seqOrderId,
			EventTimestamp: ts,
			EventType:      getCreatedOrderType(),
		})
		return records
	} else {
		g.seqShipId++
		// abort order
		if rand.Intn(100) < 10 {
			return []sink.SinkRecord{
				&parcelEvent{
					OrderId:        g.seqShipId,
					EventType:      getAbortedOrderType(),
					EventTimestamp: ts,
				},
			}
		}

		// Ship order.
		return []sink.SinkRecord{
			&parcelEvent{
				OrderId:        g.seqShipId,
				EventType:      getShippedOrderType(),
				EventTimestamp: ts,
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

func getShippedOrderType() string {
	return "shipped"
}

func getCreatedOrderType() string {
	orderStatus := []string{"new", "acknowledged", "confirmed"}
	return orderStatus[rand.Intn(len(orderStatus))]
}

func getAbortedOrderType() string {
	orderStatus := []string{"cancelled", "rejected", "reverted", "closed"}
	return orderStatus[rand.Intn(len(orderStatus))]
}
