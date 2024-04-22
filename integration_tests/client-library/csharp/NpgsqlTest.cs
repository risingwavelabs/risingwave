using System;
using System.Data;
using System.Threading.Tasks;
using Npgsql;
using Xunit;
using NpgsqlTypes;

public class NpgsqlTest
{
    private const string ConnectionString = "Host=localhost;Port=4566;Username=root;Database=dev;ServerCompatibilityMode=NoTypeLoading";

    [Fact]
    public async Task CreateAndReadFromTable()
    {
        // Create a new connection
        using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
        {
            // Open the connection
            conn.Open();

            // Create a table if it doesn't exist
            using (NpgsqlCommand createTableCmd = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS mytable (id BIGINT PRIMARY KEY, name VARCHAR, email VARCHAR)", conn))
            {
                createTableCmd.ExecuteNonQuery();
            }

            // Insert some data
            using (NpgsqlCommand insertCmd = new NpgsqlCommand("INSERT INTO mytable (id, name, email) VALUES (@id, @name, @email)", conn))
            {
                insertCmd.Parameters.AddWithValue("id", 1);
                insertCmd.Parameters.AddWithValue("name", "John Doe");
                insertCmd.Parameters.AddWithValue("email", "john.doe@example.com");
                insertCmd.ExecuteNonQuery();
            }

            // Read the data
            using (NpgsqlCommand selectCmd = new NpgsqlCommand("SELECT * FROM mytable", conn))
            {
                using (NpgsqlDataReader reader = selectCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine($"ID: {reader["id"]}, Name: {reader["name"]}, Email: {reader["email"]}");
                    }
                }
            }
        }
    }

    [Fact]
    public async Task DataTypeTest()
    {
        // Create a new connection
        using (NpgsqlConnection conn = new NpgsqlConnection(ConnectionString))
        {
            // Open the connection
            conn.Open();

            // Create a table if it doesn't exist
            using (NpgsqlCommand createTableCmd = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS data_types (" +
                "id BIGINT PRIMARY KEY, " +
                "varchar_column VARCHAR, " +
                "text_column TEXT, " +
                "integer_column INTEGER, " +
                "smallint_column SMALLINT, " +
                "bigint_column BIGINT, " +
                "decimal_column DECIMAL, " +
                "real_column REAL, " +
                "double_column DOUBLE PRECISION, " +
                "boolean_column BOOLEAN, " +
                "date_column DATE, " +
                "time_column TIME, " +
                "timestamp_column TIMESTAMP, " +
                "timestamptz_column TIMESTAMPTZ, " +
                "interval_column INTERVAL, " +
                "jsonb_column JSONB, " +
                "bytea_column BYTEA, " +
                "array_column VARCHAR[]" +
                ")", conn))
            {
                createTableCmd.ExecuteNonQuery();
            }

            // TODO: reading nested arrays is not supported yet. See https://github.com/risingwavelabs/risingwave/pull/15614
            // Insert some data
            using (NpgsqlCommand insertCmd = new NpgsqlCommand("INSERT INTO data_types (" +
                "id, varchar_column, text_column, integer_column, smallint_column, bigint_column, decimal_column, real_column, double_column, boolean_column, date_column, time_column, timestamp_column, timestamptz_column, interval_column, jsonb_column, bytea_column" +
                ") VALUES (" +
                "@id, @varchar_column, @text_column, @integer_column, @smallint_column, @bigint_column, @decimal_column, @real_column, @double_column, @boolean_column, @date_column, @time_column, @timestamp_column, @timestamptz_column, @interval_column, @jsonb_column, @bytea_column" +
                ")", conn))
            {
                insertCmd.Parameters.AddWithValue("id", 1);
                insertCmd.Parameters.AddWithValue("varchar_column", "varchar value");
                insertCmd.Parameters.AddWithValue("text_column", "text value");
                insertCmd.Parameters.AddWithValue("integer_column", 123);
                insertCmd.Parameters.AddWithValue("smallint_column", (short)123);
                insertCmd.Parameters.AddWithValue("bigint_column", 123L);
                insertCmd.Parameters.AddWithValue("decimal_column", 123.45m);
                insertCmd.Parameters.AddWithValue("real_column", 123.45f);
                insertCmd.Parameters.AddWithValue("double_column", 123.45);
                insertCmd.Parameters.AddWithValue("boolean_column", true);
                insertCmd.Parameters.AddWithValue("date_column", new DateTime(2022, 1, 1));
                insertCmd.Parameters.AddWithValue("time_column", new TimeSpan(12, 0, 0));
                insertCmd.Parameters.AddWithValue("timestamp_column", new DateTime(2022, 1, 1, 12, 0, 0));
                insertCmd.Parameters.AddWithValue("timestamptz_column", new DateTime(2022, 1, 1, 12, 0, 0));
                insertCmd.Parameters.AddWithValue("interval_column", new NpgsqlInterval(1, 2, 3));
                insertCmd.Parameters.AddWithValue("jsonb_column", NpgsqlDbType.Jsonb, "{\"key\":\"value\"}");
                insertCmd.Parameters.AddWithValue("bytea_column", new byte[] { 0x01, 0x02, 0x03 });
                insertCmd.ExecuteNonQuery();
            }

            // Read the data
            using (NpgsqlCommand selectCmd = new NpgsqlCommand("SELECT * FROM data_types", conn))
            {
                using (NpgsqlDataReader reader = selectCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Assert.Equal(1, reader["id"]);
                        Assert.Equal("varchar value", reader["varchar_column"]);
                        Assert.Equal("text value", reader["text_column"]);
                        Assert.Equal(123, reader["integer_column"]);
                        Assert.Equal((short)123, reader["smallint_column"]);
                        Assert.Equal(123L, reader["bigint_column"]);
                        Assert.Equal(123.45m, reader["decimal_column"]);
                        Assert.Equal(123.45f, reader["real_column"]);
                        Assert.Equal(123.45, reader["double_column"]);
                        Assert.Equal(true, reader["boolean_column"]);
                        Assert.Equal(new DateTime(2022, 1, 1), reader["date_column"]);
                        Assert.Equal(new TimeSpan(12, 0, 0), reader["time_column"]);
                        Assert.Equal(new DateTime(2022, 1, 1, 12, 0, 0), reader["timestamp_column"]);
                        Assert.Equal(new DateTime(2022, 1, 1, 12, 0, 0), reader["timestamptz_column"]);
                        Assert.Equal(new NpgsqlInterval(1, 2, 3), reader["interval_column"]);
                        Assert.Equal("{\"key\":\"value\"}", reader["jsonb_column"].ToString());
                        Assert.Equal(new byte[] { 0x01, 0x02, 0x03 }, reader["bytea_column"]);
                        // Assert.Equal(new string[] { "array", "value" }, reader["array_column"]);
                    }
                }
            }

            // Drop a table.
            using (NpgsqlCommand dropTableCmd = new NpgsqlCommand("DROP TABLE data_types;", conn))
            {
                dropTableCmd.ExecuteNonQuery();
            }
        }
    }
}

