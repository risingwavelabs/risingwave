import psycopg

def test_psycopg_extended_mode():
    conn = psycopg.connect(host='localhost', port='4566', dbname='dev', user='root')
    with conn.cursor() as cur:
        cur.execute("select Array[1::bigint, 2::bigint, 3::bigint]", binary=True)
        assert cur.fetchone() == ([1, 2, 3],)

        cur.execute("select Array['foo', null, 'bar']", binary=True)
        assert cur.fetchone() == (['foo', None, 'bar'],)

        cur.execute("select ROW('123 Main St', 'New York', '10001')", binary=True)
        assert cur.fetchone() == (('123 Main St', 'New York', '10001'),)

        cur.execute("select array[ROW('123 Main St', 'New York', '10001'), ROW('234 Main St', null, '10001')]", binary=True)
        assert cur.fetchone() == ([('123 Main St', 'New York', '10001'), ('234 Main St', None, '10001')],)

if __name__ == '__main__':
    test_psycopg_extended_mode()
