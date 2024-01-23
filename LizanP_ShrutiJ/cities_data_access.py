""" Provide access to the cities table in the World DB
"""
import os
import sys

from sqlalchemy import create_engine, text

_sqlite_file = '../../../../data/world_db/world.db'
DB_URI = f'sqlite:////{_sqlite_file}'


def get_country_codes_and_names():
    """ Return country codes and names (code and name columns)

    Returns:
        list(tuple): [(code1, country1), ... (codeN, countryN)]
    """
    # NOTE: This is a query from the country table in world_db
    rows = None
    return rows


def add_city(name, country_code, district, population):
    """ Given information about the city, create a new row in the city
    table in the database

    Args:
        name (str): Case-sensitive name of a city.
        country_code (str): Valid country code
        district (str): Province, state, etc.
        population (int): Population of the city  

    Returns:
        int: ID of the city created or None 
    """ 
    # NOTE: Use the list from get_country_codes_and_names to determine if
    #       country_code is valid.  If it is not, return None
    #       The result proxy has a lastrowid property.  Assign the value of
    #       this property to the variable id.  If there was an issue creating
    #       the row, lastrowid will be assigned None
    id = None  
    return id
    
    
def get_city_by_name(name):
    """ Given the name of a city, return information about the city

    Args:
        name (str): Case-insensitive name of a city

    Returns:
        tuple: (name, country code, district, population) or None
    """
    # NOTE: Use the lower SQL function to convert the database name to 
    #       lower case.  Use the .lower() python methond to convert the 
    #       value passed in the name parameter to lower case.
    #       Select all columns from the city table.  If the city does not
    #       exist, returns None.
    row = None
    return row


def update_city_population(name, population):
    """ Given the name and population of a city, update the city's population
    in the database

    Args:
        name (str): Case-insensitive name of the city
        population (int): Populaiton of the city

    Returns:
        int: Number of rows affected.  If city was successfully updated, 
        returns 1.  If none were deleted, returns 0
    """
    # NOTE:  As you did in get_city_by_name, create a SQL delete statement
    #        that is insensitive to the case of the city name
    #        The rowcount property of the result proxy contains the number
    #        of rows affected by the delete. Assign the variable row_count
    #        to the value of this property   
    row_count = 0
    return row_count

    
def delete_city_by_name(name):
    """ Given the name of a city, delete its row from the database

    Args:
        name (str): Case-insensitive city name

    Returns:
        int: Number of rows affected.  If city was successfully deleted, 
        returns 1.  If none were deleted, returns 0
    """
    # NOTE:  As you did in get_city_by_name, create a SQL delete statement
    #        that is insensitive to the case of the city name
    #        The rowcount property of the result proxy contains the number
    #        of rows affected by the delete. Assign the variable row_count
    #        to the value of this property 
    row_count = 0
    return row_count



