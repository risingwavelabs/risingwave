#!/usr/bin/env ruby
require_relative '../rw_conn'
require_relative 'util'
require 'date'
require 'test/unit'

class SQLTest < Test::Unit::TestCase
  def setup
    @conn = get_rw_conn(host: 'risingwave-standalone')
  end

  def test_crud
    create_table(@conn)

    name = 'John Doe'
    age = 30
    salary = 50000
    tripIDs = ['12345', '67890']
    fareData = {
		  "initial_charge"    => 3.0,
		  "subsequent_charge" => 1.5,
		  "surcharge"         => 0.5,
		  "tolls"             => 2.0,
    }
    deci = 2.14159
    birthdate = Date.parse('1993-01-02')
    starttime = "20:00:00"
    timest = DateTime.now()
    timestz = DateTime.now()
    timegap = '02:00:00'

	  insert_data(@conn, name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap)
	  check_data(name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap)

    # Insert data with null values
    nullName = "Null Person";
    nullAge = 0
    nullSalary = 0
    nullTripIDs = []
    nullFareData = {}
    nullBirthdate = Date.parse('0001-01-01')
    nullStarttime = '00:00:00'
    nullTimest = DateTime.parse('0001-01-01 00:00:00')
    nullTimestz = DateTime.parse('1970-01-01T00:00:00Z')
    nullTimegap = '00:00:00';
    nullDeci = 0.0;
	  insert_data(@conn, nullName, nullAge, nullSalary, nullTripIDs, nullBirthdate, nullDeci, nullFareData, nullStarttime, nullTimest, nullTimestz, nullTimegap)
	  check_data(nullName, nullAge, nullSalary, nullTripIDs, nullBirthdate, nullDeci, nullFareData, nullStarttime, nullTimest, nullTimestz, nullTimegap)

    update_data(@conn, name, 60000)
    delete_data(@conn, name)
    drop_table(@conn)
  end

  def check_data(name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap)
    @conn.exec('FLUSH;')

    select_query = "SELECT name, age, salary, trip_id, birthdate, deci, fare, starttime, timest, timestz, timegap FROM sample_table_ruby WHERE name=$1"

    res = @conn.exec_params(select_query, [name])

    res.each do |row|
      retrievedName = row['name']
      assert_equal(name, retrievedName)

      retrievedAge = row['age'].to_i
      assert_equal(age, retrievedAge)

      retrievedSalary = row['salary'].to_i
      assert_equal(salary, retrievedSalary)

      retrievedTripIDs = row['trip_id']
      assert_equal('{' + tripIDs.join(',') + '}', retrievedTripIDs)

      retrievedBirthdate = row['birthdate']
      assert_equal(birthdate, Date.parse(retrievedBirthdate))

      retrievedDeci = row['deci']
      assert_equal(deci, retrievedDeci.to_f)

      retrievedFareData = row['fare']

      retrievedStarttime = row['starttime']
      assert_equal(starttime, retrievedStarttime)

      retrievedTimest = row['timest']
      assert_equal(timest.strftime('%Y-%m-%d %H:%M:%S%z'), DateTime.parse(retrievedTimest).strftime('%Y-%m-%d %H:%M:%S%z'))

      retrievedTimestz = row['timestz']
      assert_equal(timestz.strftime('%Y-%m-%dT%H:%M:%S%zZ'), DateTime.parse(retrievedTimestz).strftime('%Y-%m-%dT%H:%M:%S%zZ'))

      retrievedTimegap = row['timegap']
      assert_equal(timegap, retrievedTimegap)
    end
    puts "Data checked successfully."
  end
end
