#!/usr/bin/env ruby

def create_table(conn)
  query = <<-EOF
    CREATE TABLE sample_table_ruby
    (
        name              VARCHAR,
        age               INTEGER,
        salary            BIGINT,
        trip_id           VARCHAR[],
        birthdate         DATE,
        deci              DOUBLE PRECISION,
        fare              STRUCT < initial_charge DOUBLE PRECISION,
        subsequent_charge DOUBLE PRECISION,
        surcharge         DOUBLE PRECISION,
        tolls             DOUBLE PRECISION >,
        starttime         TIME,
        timest            TIMESTAMP,
        timestz           TIMESTAMPTZ,
        timegap           INTERVAL
    )
  EOF
  conn.exec(query)
  puts "Table created successfully."
end

def drop_table(conn)
  conn.exec('DROP TABLE sample_table_ruby;')
  puts "Table dropped successfully."
end

def insert_data(conn, name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap)
  insert_query = <<-EOF
    INSERT INTO sample_table_ruby (name, age, salary, trip_id, birthdate, deci, fare, starttime, timest, timestz, timegap)
    VALUES ($1, $2, $3, $4, $5, $6, ROW($7, $8, $9, $10), $11, $12, $13, $14);
  EOF
  conn.exec_params(insert_query, [name, age, salary,
    "{#{tripIDs.join(',')}}",
    birthdate,
    deci,
    fareData["initial_charge"],
    fareData["subsequent_charge"],
    fareData["surcharge"],
    fareData["tolls"],
    starttime,
    timest.strftime('%Y-%m-%d %H:%M:%S'),
    timestz,
    timegap
  ])

  puts "Data inserted successfully."
end

def update_data(conn, name, salary)
  update_query = "UPDATE sample_table_ruby SET salary=$1 WHERE name=$2"

  conn.exec_params(update_query, [salary, name])
  puts "Data updated successfully."
end

def delete_data(conn, name)
  conn.exec_params('DELETE FROM sample_table_ruby WHERE name=$1;', [name])

  puts "Data deletion successfully."
end
