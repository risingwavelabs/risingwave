from sqlalchemy import Column, BigInteger, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pytest
# Create a base class for declarative class definitions
Base = declarative_base()


class User(Base):
    # Define a simple User class as an example
    __tablename__ = 'users'

    id = Column('id', BigInteger, primary_key=True)
    name = Column('name', String)
    age = Column('age', Integer)


# Pytest fixture to create and destroy the database session
@pytest.fixture
def db_session():
    DB_URI = 'risingwave+psycopg2://root@risingwave-standalone:4566/dev'
    # Create an SQLAlchemy engine to manage connections to the database
    engine = create_engine(DB_URI)

    # The automatically created table is incorrect. The BigInteger will be translated into BIGSERIAL somehow, which is not supported.
    create_table = """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            age INTEGER
        )
    """
    with engine.connect() as conn:
        conn.execute(create_table)
        conn.execute('SET RW_IMPLICIT_FLUSH=true')

    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = Session()
    yield session
    session.close()

    Base.metadata.drop_all(engine)


# Pytest test functions to perform CRUD operations
def test_create_user(db_session):
    new_user = User(id=1, name='John Doe', age=30)
    db_session.add(new_user)
    db_session.commit()
    assert new_user.id is not None

    all_users = db_session.query(User).all()
    assert len(all_users) > 0


def test_delete_user(db_session):
    user_to_delete = db_session.query(User).filter_by(name='John Doe').first()
    if user_to_delete:
        db_session.delete(user_to_delete)
        db_session.commit()
        deleted_user = db_session.query(User).get(user_to_delete.id)
        assert deleted_user is None
