from sqlalchemy import create_engine, text, inspect
import pytest

from red30_data_access import (
    create_salesperson,
    read_salesperson,
    read_salespersons,
    update_salesperson,
    delete_salesperson
)
import red30_data_access as rda

# DB URI for the test database
rda.DB_URI = f'sqlite:///test_red30.db'  

# pytest.fixtures are functions which are executed before each test
# In this function, the yield statement means the code after the yield
# will be executed after each test is completed
#
@pytest.fixture
def setup_database():
    # Fixture to set up a clean database for each test.
    engine = create_engine(rda.DB_URI)
    inspector = inspect(engine)
    if not "SalesPerson" in inspector.get_table_names():
        with engine.connect() as conn:
            # Create the "SalesPerson" table
            conn.execute(text("""
                CREATE TABLE "SalesPerson" (
                    first_name TEXT (25),
                    last_name TEXT (50),
                    email_address TEXT,
                    city TEXT,
                    state TEXT
                )
            """))
    else:
        with engine.connect() as conn:
            conn.execute(text('DELETE FROM SalesPerson'))
            conn.commit()
    # Yield execution to the test function
    yield
    # Return from test function and teardown from test
    # That is, drop the table after each test
    with engine.connect() as conn:
        conn.execute('DROP TABLE IF EXISTS SalesPerson')

def test_connection(setup_database):
    assert True

    
def test_create_salesperson(setup_database):
    id = create_salesperson('John', 'Doe', 'john.doe@example.com', 'CityA', 'StateX')
    assert id != None
    assert id >= 0


def test_read_salesperson(setup_database):
    expected = 'John'
    create_salesperson('John', 'Doe', 'john.doe@example.com', 'CityA', 'StateX')
    sales_person = read_salesperson('John', 'Doe')
    assert expected == sales_person[0]


def test_read_salesperson_does_not_exist(setup_database):
    sales_person = read_salesperson('John', 'Doe')
    assert sales_person == None


def test_read_salespersons(setup_database):
    expected = 'John'
    create_salesperson('John', 'Doe', 'john.doe@example.com', 'CityA', 'StateX')
    create_salesperson('Jane', 'Doe', 'jane.doe@example.com', 'CityA', 'StateX')
    
    result = read_salespersons()
    assert len(result) == 2
    assert result[0][0] == expected


def test_update_salesperson(setup_database):
    create_salesperson('John', 'Doe', 'john.doe@example.com', 'CityA', 'StateX')
    row_count = update_salesperson('john.doe@example.com', 'CityB', 'StateY')
    assert row_count == 1
    result = read_salespersons()
    assert result[0][3] == 'CityB'
    assert result[0][4] == 'StateY'


def test_delete_salesperson(setup_database):
    expected = None
    create_salesperson('Jane', 'Doe', 'jane.doe@example.com', 'CityA', 'StateX')
    assert read_salesperson('Jane', 'Doe')[0] == 'Jane'
    delete_salesperson('jane.doe@example.com')
    actual = read_salesperson('Jane', 'Doe')
    assert expected == actual