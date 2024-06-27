import {checkInsertedData, createTable, deleteDataByName, dropTable, insertData, updateSalaryData} from './util.js'

export async function test_crud(client) {
    await createTable(client)

    let name = "John Doe"
    let age = 30
    let salary = BigInt(50000)
    let tripIDs = ['12345', '67890']
    let fareData = {
        initial_charge: 3.0,
        subsequent_charge: 1.5,
        surcharge: 0.5,
        tolls: 2.0,
    }
    let deci = 3.14159
    let birthdate = "1993-05-15"
    let starttime = "18:20:00"
    let timest = "1993-05-15 00:00:00"
    let timestz = new Date()
    let timegap = "5 hours"
    await insertData(client, name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap)
    await checkInsertedData(client, name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap)

    // Insert data with null values
    let nullName = "Null Person"
    let nullAge = 0
    let nullSalary = BigInt(0)
    let nullTripIDs = []
    let nullFareData = {}
    let nullBirthDate = "0001-01-01"
    let nullStarttime = "00:00:00"
    let nullTimest = "0001-01-01 00:00:00"
    let nullTimestz = new Date(0)
    let nullTimegap = "0"
    let nullDeci = 0.0
    await insertData(client, nullName, nullAge, nullSalary, nullTripIDs, nullBirthDate, nullDeci, nullFareData, nullStarttime, nullTimest, nullTimestz, nullTimegap)

    await checkInsertedData(client, nullName, nullAge, nullSalary, nullTripIDs, nullBirthDate, nullDeci, nullFareData, nullStarttime, nullTimest, nullTimestz, nullTimegap)

    await updateSalaryData(client, name, BigInt(60000))
    await deleteDataByName(client, name)
    await dropTable(client)
}
