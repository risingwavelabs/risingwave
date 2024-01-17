import * as chai from "chai";

const assert = chai.assert

export async function createTable(client) {
    let createTableQuery = `
        CREATE TABLE sample_table_nodejs
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
            timegap INTERVAL
        )
    `;
    await client.query(createTableQuery)
    console.log("Table created successfully.")
}

export async function dropTable(client) {
    let query = "drop table IF EXISTS sample_table_nodejs;"
    await client.query(query)
    console.log("Table dropped successfully.")
}

export async function insertData(client, name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap) {
    let insertDateQuery = {
        text: `
            INSERT INTO sample_table_nodejs (name, age, salary, trip_id, birthdate, deci, fare, starttime, timest,
                                             timestz, timegap)
            VALUES ($1, $2, $3, $4, $5, $6, ROW($7, $8, $9, $10), $11, $12, $13, $14);
        `,
        values: [name, age, salary, tripIDs, birthdate, deci, fareData.initial_charge, fareData.subsequent_charge, fareData.surcharge, fareData.tolls, starttime, timest, timestz, timegap]
    }
    await client.query(insertDateQuery)
    console.log("Data inserted successfully.")
}

export async function checkInsertedData(client, name, age, salary, tripIDs, birthdate, deci, fareData, starttime, timest, timestz, timegap) {
    await client.query("FLUSH;")

    let query = "SELECT name, age, salary, trip_id, birthdate, deci, fare, starttime, timest, timestz, timegap FROM sample_table_nodejs WHERE name=$1"
    let res = await client.query(query, [name])
    let row = res.rows[0]

    let retrievedName = row.name
    assert.deepStrictEqual(retrievedName, name)

    let retrievedAge = row.age
    assert.deepStrictEqual(retrievedAge, age)

    let retrievedSalary = row.salary
    assert.deepStrictEqual(retrievedSalary, salary)

    let retrievedTripIDs = row.trip_id
    console.log(retrievedTripIDs)
    assert.deepStrictEqual(retrievedTripIDs, tripIDs)

    let retrievedBirthdate = row.birthdate
    assert.deepStrictEqual(retrievedBirthdate, birthdate)

    let retrievedDeci = row.deci
    assert.deepStrictEqual(retrievedDeci, deci)

    let retrievedFareData = row.fare

    let retrievedStarttime = row.starttime
    assert.deepStrictEqual(retrievedStarttime, starttime)

    let retrievedTimest = row.timest
    assert.deepStrictEqual(retrievedTimest, timest)

    let retrievedTimestz = row.timestz
    assert.deepStrictEqual(retrievedTimestz, timestz)

    let retrievedTimegap = row.timegap.toPostgres()
    assert.deepStrictEqual(retrievedTimegap, timegap)

    console.log("Data checked successfully.")
}

export async function updateSalaryData(client, name, salary) {
    let query = `
        UPDATE sample_table_nodejs
        SET salary=$1
        WHERE name = $2;
    `
    await client.query(query, [salary, name])
    console.log("Data updated successfully.")
}

export async function deleteDataByName(client, name) {
    let query = `
        DELETE
        FROM sample_table_nodejs
        WHERE name = $1;
    `
    await client.query(query, [name])
    console.log("Data deleted successfully.")
}
