import {Pool} from 'pg';

describe('PgwireTest', () => {
  test('simple select', async () => {
    const pool = new Pool({
      host: '127.0.0.1',
      database: 'dev',
      port: 4566,
      user: 'root',
    });
    try {
      const conn = await pool.connect();
      try {
        const res = await conn.query({
          text: 'SELECT $1::int AS number',
          values: ['1'],
        });
        expect(res.rowCount).toBe(1);
      } finally {
        await conn.release();
      }
    } finally {
      await pool.end();
    }
  });
});
