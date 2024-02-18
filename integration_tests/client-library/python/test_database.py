import pytest
from client import Client
from crud import SampleTableCrud
from materializeview import MaterializeView


@pytest.fixture
def db_connection():
    db = Client(
        host="risingwave-standalone",
        port="4566",
        database="dev",
        user="root",
        password=""
    )
    yield db
    db.disconnect()


@pytest.fixture
def crud_instance():
    return SampleTableCrud(
        host="risingwave-standalone",
        port="4566",
        database="dev",
        user="root",
        password=""
    )


@pytest.fixture
def mv_instance():
    return MaterializeView(
        host="risingwave-standalone",
        port="4566",
        database="dev",
        user="root",
        password=""
    )


def test_connect(db_connection):
    assert db_connection.connect() != None


def test_disconnect(db_connection):
    db_connection.connect()
    db_connection.disconnect()
    assert db_connection.connection is None


def test_table_creation(crud_instance, db_connection):
    cursor = db_connection.connect()
    cursor.execute("SET TRANSACTION READ WRITE;")
    crud_instance.create_table()

    cursor.execute("FLUSH;")
    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'sample_table_py';")
    result = cursor.fetchone()[0]
    cursor.close()
    assert result == 'sample_table_py'


def test_data_insertion(crud_instance, db_connection):
    crud_instance.insert_data("John Doe", 25, 10000)

    cursor = db_connection.connect()
    cursor.execute("FLUSH;")
    cursor.execute("SELECT COUNT(*) FROM sample_table_py;")
    result = cursor.fetchone()
    result = result[0]
    cursor.close()

    assert result == 1


def test_data_updation(crud_instance, db_connection):
    crud_instance.update_data("John Doe", 12000)

    cursor = db_connection.connect()
    cursor.execute("FLUSH;")
    cursor.execute("SELECT salary FROM sample_table_py WHERE name='John Doe';")
    result = cursor.fetchone()
    result = result[0]
    cursor.close()
    assert result == 12000


def test_data_deletion(crud_instance, db_connection):
    crud_instance.delete_data("John Doe")

    cursor = db_connection.connect()
    cursor.execute("FLUSH;")
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM sample_table_py WHERE name = 'John Doe');")
    result = cursor.fetchone()
    result = result[0]
    cursor.close()

    assert result == True


def test_table_drop(crud_instance, db_connection):
    crud_instance.table_drop()

    cursor = db_connection.connect()
    cursor.execute("FLUSH;")
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'sample_table_py');")
    result = cursor.fetchone()
    result = result[0]
    cursor.close()

    assert result is False


def test_mv_creation(mv_instance, db_connection):
    mv_instance.create_mv()
    cursor = db_connection.connect()
    cursor.execute("FLUSH;")
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'average_salary_view_py');")
    result = cursor.fetchone()[0]
    cursor.close()
    assert result is True


def test_mv_updation(db_connection, crud_instance):
    crud_instance.insert_data("Stark", 25, 13000)
    cursor = db_connection.connect()
    cursor.execute("FLUSH;")
    cursor.execute(
        "SELECT average_salary FROM average_salary_view_py WHERE age=25;")
    result = cursor.fetchone()[0]
    cursor.close()
    # assert result == 11250
    assert result == 12000


def test_mv_drop(crud_instance, mv_instance, db_connection):
    mv_instance.drop_mv()
    crud_instance.table_drop()
    cursor = db_connection.connect()
    cursor.execute("FLUSH;")
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'average_salary_view_py');")
    result = cursor.fetchone()
    result = result[0]
    cursor.close()

    assert result is False
