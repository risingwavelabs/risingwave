package gen

import (
	"context"
	"datagen/sink"
	"datagen/sink/kafka"
	"datagen/sink/kinesis"
	"datagen/sink/mysql"
	"datagen/sink/postgres"
	"datagen/sink/pulsar"

	"gonum.org/v1/gonum/stat/distuv"
)

type GeneratorConfig struct {
	Postgres postgres.PostgresConfig
	Mysql    mysql.MysqlConfig
	Kafka    kafka.KafkaConfig
	Pulsar   pulsar.PulsarConfig
	Kinesis  kinesis.KinesisConfig

	// Whether to print the content of every event.
	PrintInsert bool
	// The datagen mode, e.g. "ad-ctr".
	Mode string
	// The sink type.
	Sink string
	// The throttled requests-per-second.
	Qps int

	// Whether the tail probability is high.
	// If true, We will use uniform distribution for randomizing values.
	HeavyTail bool

	// The record format, used when the sink is a message queue.
	Format string
}

type LoadGenerator interface {
	KafkaTopics() []string

	Load(ctx context.Context, outCh chan<- sink.SinkRecord)
}

const RwTimestampLayout = "2006-01-02 15:04:05.07+01:00"

type RandDist interface {
	// Rand returns a random number ranging from [0, max].
	Rand(max float64) float64
}

func NewRandDist(cfg GeneratorConfig) RandDist {
	if cfg.HeavyTail {
		return UniformDist{}
	} else {
		return PoissonDist{}
	}
}

type UniformDist struct {
	u map[float64]distuv.Uniform
}

func (ud UniformDist) Rand(max float64) float64 {
	if ud.u == nil {
		ud.u = make(map[float64]distuv.Uniform)
	}
	_, ok := ud.u[max]
	if !ok {
		ud.u[max] = distuv.Uniform{
			Min: 0,
			Max: max,
		}
	}
	gen_num := ud.u[max].Rand()
	return gen_num
}

// A more real-world distribution. The tail will have lower probability..
type PoissonDist struct {
	ps map[float64]distuv.Poisson
}

func (pd PoissonDist) Rand(max float64) float64 {
	if pd.ps == nil {
		pd.ps = make(map[float64]distuv.Poisson)
	}
	_, ok := pd.ps[max]
	if !ok {
		pd.ps[max] = distuv.Poisson{
			Lambda: max / 2,
		}
	}
	gen_num := pd.ps[max].Rand()
	return gen_num
}
