import psycopg
from decimal import Decimal
import math
import unittest
import datetime
import zoneinfo

PG_HOST = 'localhost'
PG_PORT = 4566
PG_DBNAME = 'dev'
PG_USER = 'root'

class TestPsycopgExtendedMode(unittest.TestCase):
    def test_psycopg_extended_mode(self):
        with psycopg.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME, user=PG_USER) as conn:
            with conn.cursor() as cur:
                # Boolean
                cur.execute("select true, false, null::boolean", binary=True)
                self.assertEqual(cur.fetchone(), (True, False, None))

                # Integer types
                cur.execute("select 1::smallint, 2::integer, 3::bigint", binary=True)
                self.assertEqual(cur.fetchone(), (1, 2, 3))

                # Decimal/Numeric types
                cur.execute("select 1.23::decimal, 2.5::real, 3.45::double precision", binary=True)
                self.assertEqual(cur.fetchone(), (Decimal('1.23'), 2.5, 3.45))

                # String
                cur.execute("select 'hello'::varchar, null::varchar", binary=True)
                self.assertEqual(cur.fetchone(), ('hello', None))

                # Date/Time types
                cur.execute("select '2023-01-01'::date, '12:34:56'::time, '2023-01-01 12:34:56'::timestamp, '2023-01-01 12:34:56+00'::timestamptz", binary=True)
                self.assertEqual(cur.fetchone(), (datetime.date(2023, 1, 1), datetime.time(12, 34, 56), datetime.datetime(2023, 1, 1, 12, 34, 56), datetime.datetime(2023, 1, 1, 20, 34, 56, tzinfo=zoneinfo.ZoneInfo(key='Asia/Shanghai'))))

                # Interval
                cur.execute("select '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval", binary=True)
                self.assertEqual(cur.fetchone(), (datetime.timedelta(days=428, seconds=14706),))

                # Byte array
                cur.execute("select '\\xDEADBEEF'::bytea", binary=True)
                self.assertEqual(cur.fetchone(), (b'\xDE\xAD\xBE\xEF',))

                cur.execute("select '\\x'::bytea", binary=True)
                self.assertEqual(cur.fetchone(), (b'',))

                # Array
                cur.execute("select ARRAY[true, false, null]::boolean[]", binary=True)
                self.assertEqual(cur.fetchone(), ([True, False, None],))

                cur.execute("select ARRAY[1, 2, 3]::smallint[]", binary=True)
                self.assertEqual(cur.fetchone(), ([1, 2, 3],))

                cur.execute("select ARRAY[1, 2, 3]::integer[]", binary=True)
                self.assertEqual(cur.fetchone(), ([1, 2, 3],))

                cur.execute("select ARRAY[1, 2, 3]::bigint[]", binary=True)
                self.assertEqual(cur.fetchone(), ([1, 2, 3],))

                cur.execute("select ARRAY[1.1, 2.2, 3.3]::decimal[]", binary=True)
                self.assertEqual(cur.fetchone(), ([Decimal('1.1'), Decimal('2.2'), Decimal('3.3')],))

                cur.execute("select ARRAY[1.1, 2.2, 3.3]::real[]", binary=True)
                result = cur.fetchone()[0]  # Fetch once and store the result
                self.assertAlmostEqual(result[0], 1.1, places=6)
                self.assertAlmostEqual(result[1], 2.2, places=6)
                self.assertAlmostEqual(result[2], 3.3, places=6)

                cur.execute("select ARRAY[1.1, 2.2, 3.3]::double precision[]", binary=True)
                result = cur.fetchone()[0]  # Fetch once and store the result
                self.assertAlmostEqual(result[0], 1.1, places=6)
                self.assertAlmostEqual(result[1], 2.2, places=6)
                self.assertAlmostEqual(result[2], 3.3, places=6)

                cur.execute("select ARRAY['foo', 'bar', null]::varchar[]", binary=True)
                self.assertEqual(cur.fetchone(), (['foo', 'bar', None],))

                cur.execute("select ARRAY['\\xDEADBEEF'::bytea, '\\x0102'::bytea]", binary=True)
                self.assertEqual(cur.fetchone(), ([b'\xDE\xAD\xBE\xEF', b'\x01\x02'],))

                cur.execute("select ARRAY['2023-01-01', '2023-01-02']::date[]", binary=True)
                self.assertEqual(cur.fetchone(), ([datetime.date(2023, 1, 1), datetime.date(2023, 1, 2)],))

                cur.execute("select ARRAY['12:34:56', '23:45:01']::time[]", binary=True)
                self.assertEqual(cur.fetchone()[0], [datetime.time(12, 34, 56), datetime.time(23, 45, 1)])

                cur.execute("select ARRAY['2023-01-01 12:34:56', '2023-01-02 23:45:01']::timestamp[]", binary=True)
                self.assertEqual(cur.fetchone()[0], [datetime.datetime(2023, 1, 1, 12, 34, 56), datetime.datetime(2023, 1, 2, 23, 45, 1)])

                cur.execute("select ARRAY['2023-01-01 12:34:56+00', '2023-01-02 23:45:01+00']::timestamptz[]", binary=True)
                self.assertEqual(cur.fetchone()[0], [datetime.datetime(2023, 1, 1, 12, 34, 56, tzinfo=datetime.timezone.utc), datetime.datetime(2023, 1, 2, 23, 45, 1, tzinfo=datetime.timezone.utc)])

                cur.execute("select ARRAY['{\"a\": 1}'::jsonb, '{\"b\": 2}'::jsonb]", binary=True)
                self.assertEqual(cur.fetchone(), ([{'a': 1}, {'b': 2}],))

                # Struct
                cur.execute("select ROW('123 Main St'::varchar, 'New York'::varchar, 10001)", binary=True)
                self.assertEqual(cur.fetchone(), (('123 Main St', 'New York', 10001),))

                cur.execute("select array[ROW('123 Main St'::varchar, 'New York'::varchar, 10001), ROW('234 Main St'::varchar, null, 10002)]", binary=True)
                self.assertEqual(cur.fetchone(), ([('123 Main St', 'New York', 10001), ('234 Main St', None, 10002)],))

                # Numeric
                cur.execute("select 'NaN'::numeric, 'NaN'::real, 'NaN'::double precision", binary=True)
                result = cur.fetchone()
                self.assertTrue(result[0].is_nan())
                self.assertTrue(math.isnan(result[1]))
                self.assertTrue(math.isnan(result[2]))

                cur.execute("select 'Infinity'::numeric, 'Infinity'::real, 'Infinity'::double precision", binary=True)
                self.assertEqual(cur.fetchone(), (float('inf'), float('inf'), float('inf')))

                cur.execute("select '-Infinity'::numeric, '-Infinity'::real, '-Infinity'::double precision", binary=True)
                self.assertEqual(cur.fetchone(), (float('-inf'), float('-inf'), float('-inf')))

                # JSONB
                cur.execute("select '{\"name\": \"John\", \"age\": 30, \"city\": null}'::jsonb", binary=True)
                self.assertEqual(cur.fetchone(), ({'name': 'John', 'age': 30, 'city': None},))

                cur.execute("select '{\"scores\": [85.5, 90, null], \"passed\": true}'::jsonb", binary=True)
                self.assertEqual(cur.fetchone(), ({'scores': [85.5, 90, None], 'passed': True},))

                cur.execute("select '[{\"id\": 1, \"value\": null}, {\"id\": 2, \"value\": \"test\"}]'::jsonb", binary=True)
                self.assertEqual(cur.fetchone(), ([{'id': 1, 'value': None}, {'id': 2, 'value': 'test'}],))

if __name__ == '__main__':
    unittest.main()
