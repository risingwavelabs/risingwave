package postgres

import (
	"context"
	"database/sql"
	"datagen/sink"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgresConfig struct {
	DbHost   string
	Database string
	DbPort   int
	DbUser   string
}

type PostgresSink struct {
	db *sql.DB
}

func OpenPostgresSink(cfg PostgresConfig) (*PostgresSink, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s:@%s:%d/%s?sslmode=disable",
		cfg.DbUser, cfg.DbHost, cfg.DbPort, cfg.Database))
	if err != nil {
		return nil, err
	}
	return &PostgresSink{db}, nil
}

func (p *PostgresSink) Prepare(topics []string) error {
	return nil
}

func (p *PostgresSink) Close() error {
	return p.db.Close()
}

func (p *PostgresSink) WriteRecord(ctx context.Context, format string, record sink.SinkRecord) error {
	query := record.ToPostgresSql()
	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		err = fmt.Errorf("failed to execute query '%s': %s", query, err)
	}
	return err
}
