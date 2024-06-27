import { rw_client } from '../index.js'
import { test_crud } from "./crud_test.js";

let client = rw_client()

beforeEach(async function () {
    await client.connect()
})

afterEach(async function () {
    await client.end()
})

describe('nodejs client test', function () {
    it('crud test', async function () {
        await test_crud(client)
    })
});
