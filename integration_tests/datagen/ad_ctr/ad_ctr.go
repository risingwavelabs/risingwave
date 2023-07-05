package ad_ctr

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type adImpressionEvent struct {
	sink.BaseSinkRecord

	BidId               int64  `json:"bid_id"`
	AdId                int64  `json:"ad_id"`
	ImpressionTimestamp string `json:"impression_timestamp"`
}

func (r *adImpressionEvent) ToPostgresSql() string {
	return fmt.Sprintf("INSERT INTO %s (bid_id, ad_id, impression_timestamp) values ('%d', '%d', '%s')",
		"ad_impression", r.BidId, r.AdId, r.ImpressionTimestamp)
}

func (r *adImpressionEvent) ToJson() (topic string, key string, data []byte) {
	data, _ = json.Marshal(r)
	return "ad_impression", fmt.Sprint(r.BidId), data
}

type adClickEvent struct {
	sink.BaseSinkRecord

	BidId          int64  `json:"bid_id"`
	ClickTimestamp string `json:"click_timestamp"`
}

func (r *adClickEvent) ToPostgresSql() string {
	return fmt.Sprintf("INSERT INTO %s (bid_id, click_timestamp) values ('%d',  '%s')",
		"ad_click", r.BidId, r.ClickTimestamp)
}

func (r *adClickEvent) ToJson() (topic string, key string, data []byte) {
	data, _ = json.Marshal(r)
	return "ad_click", fmt.Sprint(r.BidId), data
}

type adCtrGen struct {
	faker *gofakeit.Faker
	ctr   map[int64]float64
}

func NewAdCtrGen() gen.LoadGenerator {
	return &adCtrGen{
		ctr:   make(map[int64]float64),
		faker: gofakeit.New(0),
	}
}

func (g *adCtrGen) getCtr(adId int64) float64 {
	if ctr, ok := g.ctr[adId]; ok {
		return ctr
	}
	ctr := g.faker.Float64Range(0, 1)
	g.ctr[adId] = ctr
	return ctr
}

func (g *adCtrGen) hasClick(adId int64) bool {
	return g.faker.Float64Range(0, 1) < g.getCtr(adId)
}

func (g *adCtrGen) generate() []sink.SinkRecord {
	bidId, _ := strconv.ParseInt(g.faker.DigitN(8), 10, 64)
	adId := int64(g.faker.IntRange(1, 10))

	events := []sink.SinkRecord{
		&adImpressionEvent{
			BidId:               bidId,
			AdId:                adId,
			ImpressionTimestamp: time.Now().Format(gen.RwTimestampLayout),
		},
	}
	if g.hasClick(adId) {
		randomDelay := time.Duration(g.faker.IntRange(1, 10) * int(time.Second))
		events = append(events, &adClickEvent{
			BidId:          bidId,
			ClickTimestamp: time.Now().Add(randomDelay).Format(gen.RwTimestampLayout),
		})
	}
	return events
}

func (g *adCtrGen) KafkaTopics() []string {
	return []string{"ad_impression", "ad_click"}
}

func (g *adCtrGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	for {
		records := g.generate()
		for _, record := range records {
			select {
			case outCh <- record:
			case <-ctx.Done():
				return
			}
		}
	}
}
