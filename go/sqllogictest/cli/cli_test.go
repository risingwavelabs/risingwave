package cli

import (
	"database/sql"
	"flag"
	"fmt"
	"testing"

	logic "github.com/singularity-data/risingwave/go/sqllogictest"
)

var testFileGlob = flag.String("file", "", "Glob of a set of test files.")
var databasePort = flag.Int("port", 0, "Port of the remote DB server.")

var postgresDBName = flag.String("pgdb", "postgres", "The database name to connect.")
var postgresUser = flag.String("pguser", "postgres", "The database user name.")
var postgresPassword = flag.String("pgpass", "postgres", "The database password.")

func TestCli(t *testing.T) {
	logic.NewDatabase = func() (*sql.DB, error) {
		dataSourceName := fmt.Sprintf("host=localhost port=%d dbname=%s sslmode=disable",
			*databasePort, *postgresDBName,
		)
		if *postgresUser != "" {
			dataSourceName += fmt.Sprintf(" user=%s", *postgresUser)
		}
		if *postgresPassword != "" {
			dataSourceName += fmt.Sprintf(" password=%s", *postgresPassword)
		}
		if *postgresDBName != "" {
			dataSourceName += fmt.Sprintf(" dbname=%s", *postgresDBName)
		}
		t.Logf("Connect to DB [%s]", dataSourceName)
		return sql.Open("postgres", dataSourceName)
	}

	logic.CleanupDatabase = func(db *sql.DB) error {
		return db.Close()
	}

	logic.RunLogicTest(t, *testFileGlob)
}
