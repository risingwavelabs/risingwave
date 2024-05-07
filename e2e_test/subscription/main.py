import subprocess
import psycopg2
import time


def execute_slt(slt):
    if slt is None or slt == "":
        return
    cmd = f"sqllogictest -p 4566 -d dev {slt}"
    print(f"Command line is [{cmd}]")
    subprocess.run(cmd,
                   shell=True,
                   check=True)
    time.sleep(3)

def create_table_subscription():
    execute_slt("./e2e_test/subscription/create_table_and_subscription.slt")

def drop_table_subscription():
    execute_slt("./e2e_test/subscription/drop_table_and_subscription.slt")

def execute_query(sql,conn):
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    rows = cur.fetchall()
    cur.close()
    return rows

def execute_insert(sql,conn):
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def check_rows_data(expect_vec,rows,status):
    row = rows[0]
    value_len  = len(row)
    for index, value in enumerate(row):
        if index == value_len  - 1:
            continue
        if index == value_len  - 2:
            assert value == status,f"expect {value} but got {status}"
            continue
        assert value == expect_vec[index],f"expect {expect_vec[index]} but got {value}"

def test_cursor_snapshot():
    print(f"test_cursor_snapshot")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([1,2],row,1)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()


def test_cursor_snapshot_log_store():
    print(f"test_cursor_snapshot_log_store")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([1,2],row,1)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row,1)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([5,5],row,1)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()

def test_cursor_since_begin():
    print(f"test_cursor_since_begin")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub since begin()",conn)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row,1)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([5,5],row,1)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([6,6],row,1)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()

def test_cursor_since_now():
    print(f"test_cursor_since_now")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub since now()",conn)
    time.sleep(2)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([6,6],row,1)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()

def test_cursor_since_rw_timestamp():
    print(f"test_cursor_since_rw_timestamp")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub since begin()",conn)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    valuelen = len(row[0])
    rw_timestamp_1 = row[0][valuelen - 1]
    check_rows_data([4,4],row,1)
    row = execute_query("fetch next from cur",conn)
    valuelen = len(row[0])
    rw_timestamp_2 = row[0][valuelen - 1] - 1
    check_rows_data([5,5],row,1)
    row = execute_query("fetch next from cur",conn)
    valuelen = len(row[0])
    rw_timestamp_3 = row[0][valuelen - 1] + 1
    check_rows_data([6,6],row,1)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)

    execute_insert(f"declare cur subscription cursor for sub since {rw_timestamp_1}",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row,1)
    execute_insert("close cur",conn)

    execute_insert(f"declare cur subscription cursor for sub since {rw_timestamp_2}",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([5,5],row,1)
    execute_insert("close cur",conn)

    execute_insert(f"declare cur subscription cursor for sub since {rw_timestamp_3}",conn)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)

    drop_table_subscription()

def test_cursor_op():
    print(f"test_cursor_op")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([1,2],row,1)
    row = execute_query("fetch next from cur",conn)
    assert row == []

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("update t1 set v2 = 10 where v1 = 4",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row,1)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row,4)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,10],row,3)
    row = execute_query("fetch next from cur",conn)
    assert row == []

    execute_insert("delete from t1 where v1 = 4",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,10],row,2)
    row = execute_query("fetch next from cur",conn)
    assert row == []

    execute_insert("close cur",conn)
    drop_table_subscription()

if __name__ == "__main__":
    test_cursor_snapshot()
    test_cursor_op()
    test_cursor_snapshot_log_store()
    test_cursor_since_rw_timestamp()
    test_cursor_since_now()
    test_cursor_since_begin()
