package mysql

import (
	"context"
	"database/sql"
	"datagen/sink"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlConfig struct {
	DbHost     string
	Database   string
	DbPort     int
	DbUser     string
	DbPassword string
}

type MysqlSink struct {
	db *sql.DB
}

func OpenMysqlSink(cfg MysqlConfig) (*MysqlSink, error) {
	fmt.Printf("Opening MySQL sink: %+v\n", cfg)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.DbUser, cfg.DbPassword, cfg.DbHost, cfg.DbPort, cfg.Database))
	if err != nil {
		return nil, err
	}
	return &MysqlSink{db}, nil
}

func (p *MysqlSink) Prepare(topics []string) error {
	return nil
}

func (p *MysqlSink) Close() error {
	return p.db.Close()
}

func (p *MysqlSink) WriteRecord(ctx context.Context, format string, record sink.SinkRecord) error {
	// MySQL's INSERT INTO is compatible with Postgres's.
	query := record.ToPostgresSql()
	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		err = fmt.Errorf("failed to execute query '%s': %s", query, err)
	}
	return err
}
