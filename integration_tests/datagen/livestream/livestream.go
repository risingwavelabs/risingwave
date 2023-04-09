package livestream

import (
	"context"
	"datagen/gen"
	"datagen/livestream/proto"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	protobuf "google.golang.org/protobuf/proto"
)

type liveClient struct {
	faker *gofakeit.Faker

	ip      string
	agent   string
	id      string
	country string
	roomId  string
}

func (c *liveClient) emulate() *liveMetric {
	longestFreezeDuration := int64(c.faker.UintRange(0, 100))
	return &liveMetric{
		Ip:                         c.ip,
		Agent:                      c.agent,
		Id:                         c.id,
		RoomId:                     c.roomId,
		Country:                    c.country,
		VideoBps:                   int64(c.faker.UintRange(1000, 1000000)),
		VideoFps:                   int64(c.faker.UintRange(30, 40)),
		VideoRtt:                   int64(c.faker.UintRange(100, 300)),
		VideoLostPps:               int64(c.faker.UintRange(0, 10)),
		VideoLongestFreezeDuration: longestFreezeDuration,
		VideoTotalFreezeDuration:   longestFreezeDuration + int64(c.faker.UintRange(0, 20)),
		ReportTimestamp:            time.Now().Format(time.RFC3339),
	}
}

func (c *liveClient) reportMetric(ctx context.Context, outCh chan<- sink.SinkRecord) {
	for {
		select {
		case <-time.NewTicker(10 * time.Second).C:
			record := c.emulate()
			select {
			case outCh <- record:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

type liveMetric struct {
	sink.BaseSinkRecord

	Ip                         string `json:"client_ip"`
	Agent                      string `json:"user_agent"`
	Id                         string `json:"user_id"`
	RoomId                     string `json:"room_id"`
	VideoBps                   int64  `json:"video_bps"`
	VideoFps                   int64  `json:"video_fps"`
	VideoRtt                   int64  `json:"video_rtt"`
	VideoLostPps               int64  `json:"video_lost_pps"`
	VideoLongestFreezeDuration int64  `json:"video_longest_freeze_duration"`
	VideoTotalFreezeDuration   int64  `json:"video_total_freeze_duration"`
	ReportTimestamp            string `json:"report_timestamp"`
	Country                    string `json:"country"`
}

func (r *liveMetric) ToPostgresSql() string {
	return fmt.Sprintf(
		`
INSERT INTO %s (client_ip, user_agent, user_id, room_id, video_bps, video_fps, video_rtt, video_lost_pps, video_longest_freeze_duration, video_total_freeze_duration, report_timestamp, country)
VALUES ('%s', '%s', '%s', '%s', %d, %d, %d, %d, %d, %d, '%s', '%s')
`,
		"live_stream_metrics",
		r.Ip, r.Agent, r.Id, r.RoomId, r.VideoBps, r.VideoFps, r.VideoRtt, r.VideoLostPps, r.VideoLongestFreezeDuration, r.VideoTotalFreezeDuration, r.ReportTimestamp, r.Country)
}

func (r *liveMetric) ToJson() (topic string, key string, data []byte) {
	data, _ = json.Marshal(r)
	return "live_stream_metrics", fmt.Sprint(r.Id), data
}

func (r *liveMetric) ToProtobuf() (topic string, key string, data []byte) {
	m := proto.LiveStreamMetrics{
		ClientIp:                   r.Ip,
		UserAgent:                  r.Agent,
		UserId:                     r.Id,
		RoomId:                     r.RoomId,
		VideoBps:                   r.VideoBps,
		VideoFps:                   r.VideoFps,
		VideoRtt:                   r.VideoRtt,
		VideoLostPps:               r.VideoLostPps,
		VideoLongestFreezeDuration: r.VideoLongestFreezeDuration,
		VideoTotalFreezeDuration:   r.VideoTotalFreezeDuration,
		ReportTimestamp:            time.Now().Unix(),
		Country:                    r.Country,
	}
	data, err := protobuf.Marshal(&m)
	if err != nil {
		panic(err)
	}
	return "live_stream_metrics", fmt.Sprint(r.Id), data
}

type liveStreamMetricsGen struct {
	faker *gofakeit.Faker
	cfg   gen.GeneratorConfig
}

func NewLiveStreamMetricsGen(cfg gen.GeneratorConfig) gen.LoadGenerator {
	return &liveStreamMetricsGen{
		faker: gofakeit.New(0),
		cfg:   cfg,
	}
}

func (g *liveStreamMetricsGen) KafkaTopics() []string {
	return []string{"live_stream_metrics"}
}

func (g *liveStreamMetricsGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	// The number of clients is roughly the QPS.
	clients := int(math.Min(float64(g.cfg.Qps), 1000))
	for i := 0; i < clients; i++ {
		go func(i int) {
			c := &liveClient{
				faker:   g.faker,
				id:      fmt.Sprint(i),
				agent:   g.faker.UserAgent(),
				ip:      fmt.Sprintf("%s:%d", g.faker.IPv4Address(), g.faker.Uint16()),
				country: g.faker.Country(),
				roomId:  fmt.Sprint(g.faker.Uint32()),
			}
			c.reportMetric(ctx, outCh)
		}(i)
	}
}
