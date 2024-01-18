import pg from 'pg';

export function rw_client() {
    let types = pg.types
    types.setTypeParser(types.builtins.TIMESTAMP, parseTimestamp)
    types.setTypeParser(types.builtins.DATE, parseDate)
    types.setTypeParser(types.builtins.INT8, parseBigint)

    return new pg.Client({
        host: 'risingwave-standalone',
        port: 4566,
        database: 'dev',
        user: 'root',
        password: '',
        types: types,
    })
}

function parseDate(val) {
    return val === null ? null : val
}

function parseTimestamp(val) {
    return val === null ? null : val
}

function parseBigint(val) {
    return val === null ? null : BigInt(val)
}
