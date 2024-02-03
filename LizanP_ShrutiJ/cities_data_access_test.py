import cities_data_access as cda
from sqlalchemy import create_engine, text, inspect
import pytest

# Create tests for each function in cities_data_access
# Use red30_data_access_test.py to help you with the development of these tests

from cities_data_access import (
    get_country_codes_and_names,
    add_city,
    get_city_by_name,
    update_city_population,
    delete_city_by_name
)

# DB URI for the test database
cda.DB_URI = f'sqlite:///../../../../data/world_db/world_test.db'

# pytest.fixtures are functions which are executed before each test
# In this function, the yield statement means the code after the yield
# will be executed after each test is completed
#
@pytest.fixture
def setup_database():
    # Fixture to set up a clean database for each test.
    engine = create_engine(cda.DB_URI)
    inspector = inspect(engine)
    if not "city" in inspector.get_table_names():
        with engine.connect() as conn:
            # Create the "city" table
            conn.execute(text("""
                CREATE TABLE "city" (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    name        TEXT,
                    countrycode TEXT,
                    district    TEXT,
                    population  INT
                )
            """))
    else:
        with engine.connect() as conn:
            conn.execute(text('DELETE FROM city'))
            conn.commit()
    # Yield execution to the test function
    yield
    # Return from test function and teardown from test
    # That is, drop the table after each test
    with engine.connect() as conn:
        conn.execute('DROP TABLE IF EXISTS city')

def test_connection(setup_database):
    assert True

def test_get_country_codes_and_names(setup_database):
    expected = ('ASM', 'American Samoa')
    actual = get_country_codes_and_names()[10]
    assert expected == actual

def test_add_city(setup_database):
    id = add_city('Ottawa', 'CAN', 'Ontario', 1000000)
    assert id != None
    assert id >= 0

def test_add_city_invalid(setup_database):
    expected = None
    actual = add_city('Ottawa', 'CANE', 'Ontario', 1000000)
    assert expected == actual

def test_get_city_by_name(setup_database):
    expected = ('Ottawa', 'CAN', 'Ontario', 1000000)
    add_city('Ottawa', 'CAN', 'Ontario', 1000000)
    actual = get_city_by_name('ottawa')
    assert expected == actual

def test_get_city_by_name_does_not_exist(setup_database):
    expected = None
    add_city('Ottawa', 'CAN', 'Ontario', 1000000)
    actual = get_city_by_name('Kingston')
    assert expected == actual

def test_update_city_population(setup_database):
    add_city('Ottawa', 'CAN', 'Ontario', 1000000)
    row_count = update_city_population('Ottawa', 1200000)
    assert row_count == 1
    result = get_city_by_name("Ottawa")
    assert result[3] == 1200000

def test_update_city_population_invalid(setup_database):
    add_city('Ottawa', 'CAN', 'Ontario', 1000000)
    row_count = update_city_population('Kingston', 1200000)
    assert row_count == 0
    result = get_city_by_name("Kingston")
    assert result == None

def test_delete_city_by_name(setup_database):
    add_city('Ottawa', 'CAN', 'Ontario', 1000000)
    expected = 1
    actual = delete_city_by_name('ottawa')
    assert expected == actual

def test_delete_city_by_name_invalid(setup_database):
    add_city('Ottawa', 'CAN', 'Ontario', 1000000)
    expected = 0
    actual = delete_city_by_name('Kingston')
    assert expected == actual
