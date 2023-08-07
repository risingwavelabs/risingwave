package nexmark

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type auction struct {
	sink.BaseSinkRecord

	Id       int    `json:"id"`
	ItemName string `json:"item_name"`
	DateTime int64  `json:"date_time"`
	Seller   int    `json:"seller"`
	Category int    `json:"category"`
}

func (r *auction) Topic() string {
	return "auction"
}

func (r *auction) Key() string {
	return fmt.Sprint(r.Id)
}

func (r *auction) ToJson() []byte {
	data, _ := json.Marshal(r)
	return data
}

type auctionGen struct {
	faker *gofakeit.Faker

	nextAuctionId int
}

func NewNexmarkGen(cfg gen.GeneratorConfig) gen.LoadGenerator {
	return &auctionGen{
		faker:         gofakeit.New(0),
		nextAuctionId: 1000,
	}
}

func (g *auctionGen) generate() sink.SinkRecord {
	g.nextAuctionId++
	return &auction{
		Id:       g.nextAuctionId,
		ItemName: g.faker.FarmAnimal(),
		DateTime: time.Now().Unix(),
		Seller:   g.faker.Number(1000, 1099),
		Category: g.faker.Number(1, 20),
	}
}

func (g *auctionGen) KafkaTopics() []string {
	// We generate the auction table only.
	return []string{"auction"}
}

func (g *auctionGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	for {
		record := g.generate()
		select {
		case outCh <- record:
		case <-ctx.Done():
			return
		}
	}
}
