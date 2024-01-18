<?php declare(strict_types=1);
require 'RWPDO.php';
require 'util.php';
use PHPUnit\Framework\TestCase;

class RWClientTest extends TestCase {
  public function testCrud() {
    $rw = new RWPDO(host: "risingwave-standalone");
    createTable($rw);
    $name = 'John Doe';
    $age = 30;
    $salary = 50000;
    $tripIDs = ['12345', '67890'];
    $fareData = [
		  "initial_charge"    => 3.0,
		  "subsequent_charge" => 1.5,
		  "surcharge"         => 0.5,
		  "tolls"             => 2.0,
    ];
    $deci = 3.14159;
    $birthdate = new DateTime("1993-01-02");
    $starttime = new DateTime("20:00:00");
    $timest = new DateTime();
    $timestz = new DateTime(timezone: new DateTimeZone('UTC'));
    $timegap = DateInterval::createFromDateString('2 hours');
    insertData($rw, $name, $age, $salary, $tripIDs, $birthdate, $deci, $fareData, $starttime, $timest, $timestz, $timegap);
    $this->checkData($rw, $name, $age, $salary, $tripIDs, $birthdate, $deci, $fareData, $starttime, $timest, $timestz, $timegap);

    // Insert data with null values
	  $nullName = "Null Person";
	  $nullAge = 0;
	  $nullSalary = 0;
	  $nullTripIDs = [];
	  $nullFareData = [];
	  $nullBirthdate = new DateTime('0001-01-01');
	  $nullStarttime = new DateTime('00:00:00');
	  $nullTimest = new DateTime('0001-01-01 00:00:00');
	  $nullTimestz = new DateTime('1970-01-01 00:00:00', timezone: new DateTimeZone('UTC'));
    $nullTimegap = DateInterval::createFromDateString('0 seconds');
	  $nullDeci = 0.0;
    insertData($rw, $nullName, $nullAge, $nullSalary, $nullTripIDs, $nullBirthdate, $nullDeci, $nullFareData, $nullStarttime, $nullTimest, $nullTimestz, $nullTimegap);
    $this->checkData($rw, $nullName, $nullAge, $nullSalary, $nullTripIDs, $nullBirthdate, $nullDeci, $nullFareData, $nullStarttime, $nullTimest, $nullTimestz, $nullTimegap);

    updateSalaryData($rw, $name, 60000);
    deleteDataByName($rw, $name);

    dropTable($rw);
  }

  function checkData(RWPDO &$rw, $name, int $age, int $salary, $tripIDs, DateTime $birthdate, $deci, $fareData, DateTime $starttime, DateTime $timest, DateTime $timestz, DateInterval $timegap) {
    $rw->exec('FLUSH;');

    $query = 'SELECT name, age, salary, trip_id, birthdate, deci, fare, starttime, timest, timestz, timegap FROM sample_table_php WHERE name=:name';
    $stmt = $rw->prepare($query);
    $stmt->execute([':name' => $name]);

    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
      $retrievedName = $row['name'];
      $this->assertEquals($retrievedName, $name);

      $retrievedAge = $row['age'];
      $this->assertEquals($retrievedAge, $age);

      $retrievedSalary = $row['salary'];
      $this->assertEquals($retrievedSalary, $salary);

      $retrievedTripIDsStr = trim($row['trip_id'], '{}');
      $retrievedTripIDs = [];
      if ($retrievedTripIDsStr !== '') {
        $retrievedTripIDs = explode(',', $retrievedTripIDsStr);
      }
      $this->assertEquals($retrievedTripIDs, $tripIDs);

      $retrievedBirthdate = new DateTime($row['birthdate']);
      $this->assertEquals($retrievedBirthdate, $birthdate);

      $retrievedDeci = (float)$row['deci'];
      $this->assertEquals($retrievedDeci, $deci);

      $retrievedFareData = $row['fare'];

      $retrievedStarttime = new DateTime($row['starttime']);
      $this->assertEquals($retrievedStarttime, $starttime);

      $retrievedTimest = new DateTime($row['timest']);
      $this->assertEquals($retrievedTimest, $timest);

      $retrievedTimestz = new DateTime($row['timestz']);
      $this->assertEquals($retrievedTimestz, $timestz);

      $retrievedTimegap = $row['timegap'];
      $this->assertEquals($retrievedTimegap, $timegap->format('%H:%I:%S'));
    }
    print("Data checked successfully.\n");
  }
}

?>
