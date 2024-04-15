<?php declare(strict_types=1);

function createTable(RWPDO &$rw) {
  $query = <<<EOT
CREATE TABLE sample_table_php (
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
)
EOT;
  $rw->exec($query);
  print("Create Table successfully!\n");
}

function dropTable(RWPDO &$rw) {
  $rw->exec('DROP TABLE sample_table_php;');
  print("Drop Table successfully!\n");
}

function insertData(RWPDO &$rw, $name, int $age, int $salary, $tripIDs, DateTime $birthdate, $deci, $fareData, DateTime $starttime, DateTime $timest, DateTime $timestz, DateInterval $timegap) {
  $insertQuery = <<<EOT
    INSERT INTO sample_table_php (name, age, salary, trip_id,  birthdate, deci, fare, starttime, timest, timestz, timegap)
    VALUES (?, ?, ?, ?, ?, ?, ROW(?, ?, ?, ?), ?, ?, ?, ?);
  EOT;
  $stmt = $rw->prepare($insertQuery);
  $stmt->execute([$name, $age, $salary,
    '{'.implode(',',$tripIDs).'}',
    $birthdate->format('Y-m-d'),
    $deci,
    $fareData['initial_charge'], $fareData['subsequent_charge'], $fareData['surcharge'], $fareData['tolls'],
    $starttime->format('H:i:s.u'),
    $timest->format('Y-m-d H:i:s.u'),
    $timestz->format('Y-m-d H:i:s.uP'),
    $timegap->format('%H:%I:%S')
    ]);
  print("Data inserted successfully.\n");
}

function updateSalaryData(RWPDO &$rw, $name, $salary) {
  $query = 'UPDATE sample_table_php SET salary=:salary WHERE name = :name;';
  $stmt = $rw->prepare($query);
  $stmt->execute([':name' => $name, ':salary' => $salary]);
  print("Data updated successfully.\n");
}

function deleteDataByName(RWPDO &$rw, $name) {
  $query = 'DELETE FROM sample_table_php WHERE name = :name;';
  $stmt = $rw->prepare($query);
  $stmt->execute([':name' => $name]);
  print("Data deleted successfully.\n");
}

?>
