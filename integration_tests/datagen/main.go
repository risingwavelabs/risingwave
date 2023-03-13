package main

import (
	"context"
	"datagen/gen"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli"
)

var cfg gen.GeneratorConfig = gen.GeneratorConfig{}

func runCommand() error {
	terminateCh := make(chan os.Signal, 1)
	signal.Notify(terminateCh, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-terminateCh
		log.Println("Cancelled")
		cancel()
	}()
	return generateLoad(ctx, cfg)
}

func main() {

	app := &cli.App{
		Commands: []cli.Command{
			{
				Name: "postgres",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:        "host",
						Usage:       "The host address of the PostgreSQL server",
						Required:    false,
						Value:       "localhost",
						Destination: &cfg.Postgres.DbHost,
					},
					cli.StringFlag{
						Name:        "db",
						Usage:       "The database where the target table is located",
						Required:    false,
						Value:       "dev",
						Destination: &cfg.Postgres.Database,
					},
					cli.IntFlag{
						Name:        "port",
						Usage:       "The port of the PostgreSQL server",
						Required:    false,
						Value:       4566,
						Destination: &cfg.Postgres.DbPort,
					},
					cli.StringFlag{
						Name:        "user",
						Usage:       "The user to Postgres",
						Required:    false,
						Value:       "root",
						Destination: &cfg.Postgres.DbUser,
					},
				},
				Action: func(c *cli.Context) error {
					cfg.Sink = "postgres"
					return runCommand()
				},
			},
			{
				Name: "mysql",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:        "host",
						Usage:       "The host address of the MySQL server",
						Required:    false,
						Value:       "localhost",
						Destination: &cfg.Mysql.DbHost,
					},
					cli.StringFlag{
						Name:        "db",
						Usage:       "The database where the target table is located",
						Required:    false,
						Value:       "mydb",
						Destination: &cfg.Mysql.Database,
					},
					cli.IntFlag{
						Name:        "port",
						Usage:       "The port of the MySQL server",
						Required:    false,
						Value:       3306,
						Destination: &cfg.Mysql.DbPort,
					},
					cli.StringFlag{
						Name:        "user",
						Usage:       "The user to MySQL",
						Required:    false,
						Value:       "mysqluser",
						Destination: &cfg.Mysql.DbUser,
					},
					cli.StringFlag{
						Name:        "password",
						Usage:       "The password to MySQL",
						Required:    false,
						Value:       "mysqlpw",
						Destination: &cfg.Mysql.DbPassword,
					},
				},
				Action: func(c *cli.Context) error {
					cfg.Sink = "mysql"
					return runCommand()
				},
			},
			{
				Name: "kafka",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:        "brokers",
						Usage:       "Kafka bootstrap brokers to connect to, as a comma separated list",
						Required:    true,
						Destination: &cfg.Kafka.Brokers,
					},
					cli.BoolFlag{
						Name:        "no-recreate",
						Usage:       "Do not recreate the Kafka topic when it exists.",
						Required:    false,
						Destination: &cfg.Kafka.NoRecreateIfExists,
					},
				},
				Action: func(c *cli.Context) error {
					cfg.Sink = "kafka"
					return runCommand()
				},
				HelpName: "datagen kafka",
			},
			{
				Name: "pulsar",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:        "brokers",
						Usage:       "Pulsar brokers to connect to, as a comma separated list",
						Required:    true,
						Destination: &cfg.Pulsar.Brokers,
					},
				},
				Action: func(c *cli.Context) error {
					cfg.Sink = "pulsar"
					return runCommand()
				},
				HelpName: "datagen pulsar",
			},
			{
				Name: "kinesis",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:        "region",
						Usage:       "The region where the Kinesis stream resides",
						Required:    true,
						Destination: &cfg.Kinesis.Region,
					},
					cli.StringFlag{
						Name:        "name",
						Usage:       "The Kinesis stream name",
						Required:    true,
						Destination: &cfg.Kinesis.StreamName,
					},
				},
				Action: func(c *cli.Context) error {
					cfg.Sink = "kinesis"
					return runCommand()
				},
				HelpName: "datagen kinesis",
			},
		},
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:        "print",
				Usage:       "Whether to print the content of every event",
				Required:    false,
				Destination: &cfg.PrintInsert,
			},
			cli.IntFlag{
				Name:        "qps",
				Usage:       "Number of messages to send per second",
				Required:    false,
				Value:       1,
				Destination: &cfg.Qps,
			},
			cli.StringFlag{
				Name:        "mode",
				Usage:       "ad-click | ad-ctr | twitter | cdn-metrics | clickstream | ecommerce | delivery | livestream",
				Required:    true,
				Destination: &cfg.Mode,
			},
			cli.StringFlag{
				Name:        "format",
				Usage:       "The output record format: json | protobuf. Used when the sink is a message queue.",
				Value:       "json",
				Required:    false,
				Destination: &cfg.Format,
			},
			cli.BoolFlag{
				Name:        "heavytail",
				Usage:       "Whether the tail probability is high. If true We will use uniform distribution for randomizing values.",
				Required:    false,
				Destination: &cfg.HeavyTail,
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}
