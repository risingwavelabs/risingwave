import psycopg
from decimal import Decimal
import math

def test_psycopg_extended_mode():
    conn = psycopg.connect(host='localhost', port='4566', dbname='dev', user='root')
    with conn.cursor() as cur:
        # Array
        cur.execute("select Array[1::bigint, 2::bigint, 3::bigint]", binary=True)
        assert cur.fetchone() == ([1, 2, 3],)

        cur.execute("select Array['foo', null, 'bar']", binary=True)
        assert cur.fetchone() == (['foo', None, 'bar'],)

        # Byte array
        cur.execute("select '\\xDEADBEEF'::bytea", binary=True)
        assert cur.fetchone() == (b'\xDE\xAD\xBE\xEF',)

        cur.execute("select '\\x'::bytea", binary=True) 
        assert cur.fetchone() == (b'',)

        cur.execute("select array['\\xDEADBEEF'::bytea, '\\x0102'::bytea]", binary=True)
        assert cur.fetchone() == ([b'\xDE\xAD\xBE\xEF', b'\x01\x02'],)

        # Struct
        cur.execute("select ROW('123 Main St'::varchar, 'New York'::varchar, 10001)", binary=True)
        assert cur.fetchone() == (('123 Main St', 'New York', 10001),)

        cur.execute("select array[ROW('123 Main St'::varchar, 'New York'::varchar, 10001), ROW('234 Main St'::varchar, null, 10002)]", binary=True)
        assert cur.fetchone() == ([('123 Main St', 'New York', 10001), ('234 Main St', None, 10002)],)

        # Numeric
        cur.execute("select 'NaN'::numeric, 'NaN'::real, 'NaN'::double precision", binary=True)
        result = cur.fetchone()
        assert result[0].is_nan()
        assert math.isnan(result[1])
        assert math.isnan(result[2])

        cur.execute("select 'Infinity'::numeric, 'Infinity'::real, 'Infinity'::double precision", binary=True)
        assert cur.fetchone() == (float('inf'), float('inf'), float('inf'))

        cur.execute("select '-Infinity'::numeric, '-Infinity'::real, '-Infinity'::double precision", binary=True)
        assert cur.fetchone() == (float('-inf'), float('-inf'), float('-inf'))

        # JSONB
        cur.execute("select '{\"name\": \"John\", \"age\": 30, \"city\": null}'::jsonb", binary=True)
        assert cur.fetchone() == ({'name': 'John', 'age': 30, 'city': None},)

        cur.execute("select '{\"scores\": [85.5, 90, null], \"passed\": true}'::jsonb", binary=True) 
        assert cur.fetchone() == ({'scores': [85.5, 90, None], 'passed': True},)

        cur.execute("select '[{\"id\": 1, \"value\": null}, {\"id\": 2, \"value\": \"test\"}]'::jsonb", binary=True)
        assert cur.fetchone() == ([{'id': 1, 'value': None}, {'id': 2, 'value': 'test'}],)

if __name__ == '__main__':
    test_psycopg_extended_mode()
