package pgx_test

import (
	"context"
	"fmt"
	"log"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func connect() (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig("postgres://root@risingwave-standalone:4566/dev")
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
	// TODO: Investigate into why simple protocol is required
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to database: %w", err)
	}
	return conn, nil
}

func disconnect(conn *pgx.Conn) {
	conn.Close(context.Background())
	fmt.Println("Disconnected from the database.")
}

func createTable(t *testing.T, conn *pgx.Conn) {
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS sample_table_go (
			name VARCHAR,
			age INTEGER,
			salary BIGINT,
			trip_id VARCHAR[],
			birthdate DATE,
			deci DOUBLE PRECISION,
			fare STRUCT <
				initial_charge DOUBLE PRECISION,
				subsequent_charge DOUBLE PRECISION,
				surcharge DOUBLE PRECISION,
				tolls DOUBLE PRECISION
            >,
			starttime TIME,
			timest TIMESTAMP,
			timestz TIMESTAMPTZ,
			timegap INTERVAL
		);
	`
	_, err := conn.Exec(context.Background(), createTableQuery)
	assert.Nil(t, err, "Table creation failed")
	fmt.Println("Table created successfully.")
}
func insertData(t *testing.T, conn *pgx.Conn, name string, age int, salary int64, tripIDs []string, birthdate string, deci float64, fareData map[string]float64, starttime string, timest time.Time, timestz time.Time, timegap time.Duration) {
	insertDataQuery := `
		INSERT INTO sample_table_go (name, age, salary, trip_id,  birthdate, deci, fare, starttime, timest, timestz, timegap)
		VALUES ($1, $2, $3, $4, $5, $6, ROW( $7, $8, $9, $10), $11, $12, $13, $14);
	`
	_, err := conn.Exec(context.Background(), insertDataQuery,
		name, age, salary, tripIDs,
		birthdate, deci, fareData["initial_charge"], fareData["subsequent_charge"], fareData["surcharge"], fareData["tolls"],
		starttime,
		timest.Format("2006-01-02 15:04:05.000000"),
		timestz.Format("2006-01-02 15:04:05.000000"),
		timegap,
	)
	fmt.Println(timest)
	fmt.Println(timestz)
	assert.Nil(t, err, "Data insertion failed")
	fmt.Println("Data inserted successfully.")
}

func updateData(conn *pgx.Conn, name string, salary int64) {
	updateDataQuery := `
		UPDATE sample_table_go
		SET salary=$1
		WHERE name=$2;
	`
	_, err := conn.Exec(context.Background(), updateDataQuery, salary, name)
	if err != nil {
		log.Fatalf("Data updation failed: %v", err)
	}
	fmt.Println("Data updated successfully.")
}

func deleteData(conn *pgx.Conn, name string) {
	deleteDataQuery := `
		DELETE FROM sample_table_go WHERE name=$1;
	`
	_, err := conn.Exec(context.Background(), deleteDataQuery, name)
	if err != nil {
		log.Fatalf("Data deletion failed: %v", err)
	}
	fmt.Println("Data deletion successfully.")
}

func tableDrop(conn *pgx.Conn) {
	resetQuery := `
		DROP TABLE IF EXISTS sample_table_go;
	`
	_, err := conn.Exec(context.Background(), resetQuery)
	if err != nil {
		log.Fatalf("Table drop failed: %v", err)
	}
	fmt.Println("Table dropped successfully.")
}

func TestCrud(t *testing.T) {
	conn, err := connect()
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer disconnect(conn)

	createTable(t, conn)
	name := "John Doe"
	age := 30
	salary := int64(50000)
	tripIDs := []string{"12345", "67890"}
	fareData := map[string]float64{
		"initial_charge":    3.0,
		"subsequent_charge": 1.5,
		"surcharge":         0.5,
		"tolls":             2.0,
	}
	birth := time.Date(1993, time.May, 15, 0, 0, 0, 0, time.UTC)
	birthdate := birth.Format("2006-01-02")
	start := time.Date(2023, time.August, 7, 18, 20, 0, 0, time.UTC)
	starttime := start.Format("15:04:05")
	timest := time.Now()
	timestz := time.Now().UTC()
	timegap := 2 * time.Hour
	deci := 3.14159
	insertData(t, conn, name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap)

	// Insert data with null values
	nullName := "Null Person"
	nullAge := 0
	nullSalary := int64(0)
	nullTripIDs := []string{}
	nullFareData := map[string]float64{}
	nullBirth := time.Time{}
	nullBirthdate := nullBirth.Format("2006-01-02")
	nullStart := time.Time{}
	nullStarttime := nullStart.Format("15:04:05")
	nullTimest := time.Time{}
	nullTimestz := time.Time{}
	nullTimegap := time.Duration(0)
	nullDeci := 0.0
	insertData(t, conn, nullName, nullAge, nullSalary, nullTripIDs, nullBirthdate, nullDeci, nullFareData, nullStarttime, nullTimest, nullTimestz, nullTimegap)
	checkInsertedData(t, conn, nullName, nullAge, nullSalary, nullTripIDs, nullBirthdate, nullDeci, nullFareData, nullStarttime, nullTimest, nullTimestz, nullTimegap)
	updateData(conn, "John Doe", 60000)
	deleteData(conn, "John Doe")
	tableDrop(conn)
}

func checkInsertedData(t *testing.T, conn *pgx.Conn, name string, age int, salary int64, tripIDs []string, birthdate string, deci float64, fareData map[string]float64, starttime string, timest time.Time, timestz time.Time, timegap time.Duration) {
	flushQuery := `
		FLUSH;
	`
	_, err := conn.Exec(context.Background(), flushQuery)
	assert.Nil(t, err, "Materialized View flush failed")

	query := "SELECT name, age, salary, trip_id, birthdate, deci, fare, starttime, timest, timestz, timegap FROM sample_table_go WHERE name=$1"
	row := conn.QueryRow(context.Background(), query, name)

	var retrievedName string
	var retrievedAge int
	var retrievedSalary int64
	var retrievedTripIDs []string
	var retrievedBirthdate string
	var retrievedDeci float64
	var retrievedFareData string
	var retrievedStarttime string
	var retrievedTimest time.Time
	var retrievedTimestz time.Time
	var retrievedTimegap time.Duration

	err = row.Scan(
		&retrievedName, &retrievedAge, &retrievedSalary, &retrievedTripIDs,
		&retrievedBirthdate, &retrievedDeci, &retrievedFareData, &retrievedStarttime,
		&retrievedTimest, &retrievedTimestz, &retrievedTimegap,
	)
	assert.Nil(t, err, "Error retrieving inserted data")

	if retrievedName != name ||
		retrievedAge != age ||
		retrievedSalary != salary ||
		!reflect.DeepEqual(retrievedTripIDs, tripIDs) ||
		retrievedBirthdate != birthdate ||
		math.Abs(retrievedDeci-deci) > 0.00001 ||
		retrievedStarttime != starttime ||
		retrievedTimest != timest ||
		(!retrievedTimestz.IsZero() && retrievedTimestz.UTC() != timestz.UTC()) ||
		retrievedTimegap != timegap {
		t.Errorf("Data didn't matched properly")
	}
}
