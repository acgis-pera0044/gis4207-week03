""" Command-line interface for cities_data_access.py
"""

import sys
import cities_data_access as cda

def show_usage():
    print ("""
cities_data_cli.py add city_name country_code district population
cities_data_cli.py get city_name
cities_data_cli.py delete city_name
cities_data_cli.py update_population city_name new_population
cities_data_cli.py get_country_codes""")
    sys.exit(0)

method_to_args_len = {'add':6, 'get':3, 'delete':3, 'update_population':4, 'get_country_codes':2}
methods = list(method_to_args_len.keys())

def main():
    if len(sys.argv) == 1:
        show_usage()

    method = sys.argv[1].lower()
    len_args = method_to_args_len[method]
    if len(sys.argv) != len_args:
        show_usage()
        
    if method not in methods:
        print(f'{method} not a valid method')
        print(f'Must be one of: {", ".join(methods)}')
        show_usage()

    if method == 'add':
        city_name = sys.argv[2]
        id = add_city(sys.argv[2:])
        if id >=0:
            print(f'{city_name} added')
    elif method == 'get':
        print(get_city(sys.argv[2]))
    elif method == 'delete':
        delete_city(sys.argv[2])
    elif method == 'update_population':
        count = update_city_population(sys.argv[2], int(sys.argv[3]))
    else:  # method == 'get_country_codes'
        show_country_codes()
    

def add_city(city_info):
    """ Given city information, add the city to the city table in the database
    
    Args: 
        list: city_info (i.e. name, country code, district, population) 
    """
    country_code = city_info[2]
    if not is_valid_country_code(country_code):
        print(f'{country_code} is not a valid country code')
        print('Use get_country_codes to see a list of valid country codes\n')
        show_usage()

    city_info = city_info[1:]
    id = cda.add_city(*city_info)
    if id == None:
        print('Unable to add city')
    return id
    

def get_city(name):
    """ Given the name of a city, return information about the city

    Args:
        name (str): Case-insensitive name of a city

    Returns:
        tuple: (Name, country code, district, population)
    """
    city_info = ''
    return city_info


def delete_city(name):
    """ Delete the given city

    Args:
        name (str): Name of the city to delete
    """


def update_city_population(name, population):
    """ Update the population value for a city
    
    Args: 
        name (str): Name of the city to update
        population (int): New population value
    """

def show_country_codes():
    """ Print the country codes and names to the console.
    """
    codes_and_names = cda.get_country_codes_and_names()
    for code, name in codes_and_names:
        print (code, name)


def is_valid_country_code(code_to_check):
    """ Ensure the code_to_check is valid.

    Args:
        code_to_check (str): Country code

    Returns:
        bool: True if the country code exists in the country table
    """

    #TODO: Add the necessary code to determine if code_to_check is a
    #      valid country code or not
    return False


if __name__ == '__main__':
    main()