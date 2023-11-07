import psycopg2

class SQL:
    ENTITIES = [
        'TABLE t',
        'VIEW v',
        'MATERIALIZED VIEW mv',
        'SOURCE src',
        'SINK sink',
        'DATABASE d',
        'SCHEMA s',
    ]
    CREATE_ENTITIES = [
        f'CREATE {ENTITIES[0]} (v1 INT primary key, v2 STRUCT<v1 INT, v2 STRUCT<v1 INT, v2 INT>>);',
        f'CREATE {ENTITIES[1]} AS ( SELECT * FROM t WHERE v1 = 1);',
        f'CREATE {ENTITIES[2]} AS SELECT v1, (t.v2).v1 AS v21 FROM t;',
        f'''CREATE {ENTITIES[3]} (v INT) WITH (
    connector = 'datagen',
    fields.v.kind = 'sequence',
    fields.v.start = '1',
    fields.v.end  = '10',
    datagen.rows.per.second='15',
    datagen.split.num = '1'
) FORMAT PLAIN ENCODE JSON;''',
        f'''CREATE {ENTITIES[4]} AS SELECT mv3.v1 AS v1, mv3.v21 AS v2 FROM mv AS mv3 WITH (
    connector = 'blackhole'
);''',
    f'CREATE {ENTITIES[5]};',
    f'CREATE {ENTITIES[6]};',
    ]
    ALTER_OWNER_TO = lambda owner: [
        f'ALTER {entity} OWNER TO {owner};'
        for entity in SQL.ENTITIES
    ]
    DROP_ENTITIES = [
        f'DROP {entity};'
        for entity in ENTITIES[::-1] # reverse here because root dependence on TABLE t
    ]
    CREATE_USERS = lambda users: [
        f'CREATE USER {user}'
        for user in users
    ]
    GET_OWNER = [
        f'''SELECT
    pg_roles.rolname AS owner
FROM
    pg_class
    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    JOIN pg_roles ON pg_roles.oid = pg_class.relowner
WHERE
    pg_namespace.nspname NOT LIKE 'pg_%'
    AND pg_namespace.nspname NOT LIKE 'rw_%'
    AND pg_namespace.nspname != 'information_schema'
    AND pg_class.relname = '{entity.split()[-1]}';'''
        for entity in ENTITIES[:-2] # there are only relation entries in pg_class
    ]

def connect_with_user(user):
    return psycopg2.connect(
        host='localhost',
        port='4566',
        user=user,
        database='dev',
    )

def _execute(conn, stmts, fetch=False):
    res = []
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                print(f'executing {stmt}')
                cur.execute(stmt)
                if fetch:
                    res.append(cur.fetchall())
    except psycopg2.DatabaseError as e:
        print(' -> Error')
        res = e
    finally:
        return res

def expect_ok(conn, stmts):
    result = _execute(conn, stmts)
    if isinstance(result, BaseException):
        raise Exception('Expect ok while a exception occurs.') from result

def expect_err(conn, stmts):
    result = _execute(conn, stmts)
    if not isinstance(result, BaseException):
        raise Exception(f'Expect error while the execution succeeds.', stmts)

def expect_owner(conn, expected):
    result = _execute(conn, SQL.GET_OWNER, True)
    assert all([item[0][0] == expected for item in result]), f'owner assertion failed: got {result}, expect {expected}.'

if __name__ == "__main__":
    with connect_with_user('root') as root:
        # create users for test
        expect_ok(root, SQL.CREATE_USERS(['user1', 'user2']))
        # owner is root
        expect_ok(root, SQL.CREATE_ENTITIES)
        expect_owner(root, 'root')
        # owner is user1
        expect_ok(root, SQL.ALTER_OWNER_TO('user1'))
        expect_owner(root, 'user1')

    with connect_with_user('user1') as user1:
        # owner can alter
        expect_ok(user1, SQL.ALTER_OWNER_TO('user2'))
        # owner is user2
        expect_owner(user1, 'user2')
        # expect error because only super_user or owner can alter or drop
        expect_err(user1, SQL.ALTER_OWNER_TO('user2'))
        expect_err(user1, SQL.DROP_ENTITIES)

    with connect_with_user('user2') as user2:
        # owner can drop
        expect_ok(user2, SQL.DROP_ENTITIES)

    with connect_with_user('root') as root:
        expect_ok(root, SQL.CREATE_ENTITIES)
        # super_user can alter any entity
        expect_ok(root, SQL.ALTER_OWNER_TO('user1'))
        expect_ok(root, SQL.ALTER_OWNER_TO('user2'))
        # super_user can drop any entity
        expect_ok(root, SQL.DROP_ENTITIES)

    print('Pass.')
