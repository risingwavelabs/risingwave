package pgx_test

import (
	"context"
	"fmt"
	"log"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestMaterializedView(t *testing.T) {
	conn, err := connect()
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
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
	deci := 3.14159
	insertData(t, conn, name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timest, 2*time.Hour)

	name2 := "Jane Smith"
	age2 := 28
	salary2 := int64(60000)
	tripIDs2 := []string{"98765", "54321"}
	fareData2 := map[string]float64{
		"initial_charge":    2.5,
		"subsequent_charge": 1.2,
		"surcharge":         0.4,
		"tolls":             1.5,
	}
	birth2 := time.Date(1995, time.June, 20, 0, 0, 0, 0, time.UTC)
	birthdate2 := birth2.Format("2006-01-02")
	start2 := time.Date(2023, time.August, 7, 14, 30, 0, 0, time.UTC)
	starttime2 := start2.Format("15:04:05")
	timest2 := time.Now().Add(-time.Hour * 3)
	deci2 := 2.71828
	insertData(t, conn, name2, age2, salary2, tripIDs2, birthdate2, deci2, fareData2, starttime2, timest2, timest2, 2*time.Hour)

	createMVQuery := `
		CREATE MATERIALIZED VIEW IF NOT EXISTS customer_earnings_mv_go AS
		SELECT
			name AS customer_name,
			age AS customer_age,
			SUM((fare).initial_charge + (fare).subsequent_charge + (fare).surcharge + (fare).tolls) AS total_earnings
		FROM
			sample_table_go
		GROUP BY
			name, age;
	`
	_, err = conn.Exec(context.Background(), createMVQuery)
	assert.Nil(t, err, "Materialized View creation failed")

	fmt.Println("Materialized View created successfully.")
	checkMVData(t, conn)
	dropMV(t, conn)
}

func checkMVData(t *testing.T, conn *pgx.Conn) {
	flushQuery := `
		FLUSH;
	`
	_, err := conn.Exec(context.Background(), flushQuery)
	assert.Nil(t, err, "Materialized View flush failed")
	fmt.Println("Materialized View flushed successfully.")
	checkMVQuery := `
		SELECT * FROM customer_earnings_mv_go;
	`
	rows, err := conn.Query(context.Background(), checkMVQuery)
	assert.Nil(t, err, "Materialized View data check failed")
	defer rows.Close()

	for rows.Next() {
		var customerName string
		var customerAge int
		var totalEarnings float64
		err := rows.Scan(&customerName, &customerAge, &totalEarnings)
		assert.Nil(t, err, "Error scanning MV row")

		switch customerName {
		case "John Doe":
			if customerAge != 30 || math.Abs(totalEarnings-7.0) > 0.001 {
				t.Errorf("Incorrect data for John Doe: Age=%d, Total Earnings=%.2f", customerAge, totalEarnings)
			}
		case "Jane Smith":
			if customerAge != 28 || math.Abs(totalEarnings-5.6) > 0.001 {
				t.Errorf("Incorrect data for Jane Smith: Age=%d, Total Earnings=%.2f", customerAge, totalEarnings)
			}
		}
	}
}
func dropMV(t *testing.T, conn *pgx.Conn) {
	dropMVQuery := "DROP MATERIALIZED VIEW IF EXISTS customer_earnings_mv_go;"
	_, err := conn.Exec(context.Background(), dropMVQuery)
	assert.Nil(t, err, "Materialized View drop failed")
	fmt.Println("Materialized View dropped successfully.")
}
