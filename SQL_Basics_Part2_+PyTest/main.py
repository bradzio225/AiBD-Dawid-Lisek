import numpy as np
import pickle
from pandas.core.indexes import category

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category_id) is int:
        df = pd.read_sql(f"""select film.title as title, language.name as languge, category.name as category from film
        inner join film_category on film_category.film_id = film.film_id
        inner join category on film_category.category_id = category.category_id
        inner join language  on language.language_id = film.language_id
        where category.category_id = {category_id}
        order by film.title, language.name""", con=connection)
        return df
    else:
        return None


    
def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category_id) is int:
        df = pd.read_sql(f"""select category.name as category, count(film.title) from film_category
        inner join film on film_category.film_id = film.film_id
        inner join category on film_category.category_id = category.category_id
        where category.category_id = {category_id}
        group by category.name""", con=connection)
        return df
    else:
        return None


def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(min_length) in (int, float) and type(max_length) in (int, float) and max_length > min_length: 
        df = pd.read_sql(f"""select film.length, count(film.title) from film 
            where length between {min_length} and {max_length}
            group by length""", con=connection)
        return df
    else:
        return None
    
def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(city) is str:
        df = pd.read_sql(f"""select city.city, customer.first_name, customer.last_name from city
        inner join address on city.city_id = address.city_id
        inner join customer on customer.address_id = address.address_id
        where city.city = '{city}'
        order by customer.first_name, customer.last_name""", con=connection)
        return df
    else:
        return None
    

def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(length) in (float, int):
        df = pd.read_sql(f"""select film.length, avg(payment.amount) from film
        inner join inventory on film.film_id = inventory.film_id
        inner join rental on rental.inventory_id = inventory.inventory_id
        inner join payment on payment.rental_id = rental.rental_id
        where film.length = {length}
        group by film.length""", con=connection)
        return df
    else:
        return None


def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    if type(sum_min) in (float, int) and sum_min > 0:
        df = pd.read_sql(f"""select customer.first_name, customer.last_name, sum(film.length) as sum from film
        inner join inventory on film.film_id = inventory.film_id
        inner join rental on rental.inventory_id = inventory.inventory_id
        inner join customer on rental.customer_id = customer.customer_id
        group by customer.first_name, customer.last_name
        having sum(film.length) > {sum_min}
        order by sum, customer.last_name, customer.first_name""", con=connection)
        return df
    else:
        return None  

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(name) is str:
        df = pd.read_sql(f"""select category.name as category, avg(length) as avg, sum(length) as sum, min(length) as min, max(length) as max from film
        inner join film_category on film.film_id = film_category.film_id
        inner join category on film_category.category_id = category.category_id
        where category.name = '{name}'
        group by category.name""", con=connection)
        return df
    else:
        return None

def zadanie_1() :
    df = pd.read_sql("""select film.length, count(film.title) from film
    group by film.length
    order by film.length
    """, con=connection)
    return df

def zadanie_2() :
    df = pd.read_sql("""select city.city, customer.first_name, customer.last_name  from customer
    inner join address on address.address_id = customer.address_id
    inner join city on address.city_id = city.city_id
    order by city.city
    """, con=connection)
    return df

def zadanie_3() :
    df = pd.read_sql("""select avg(film.rental_rate) as average_rental_rate from film
    """, con=connection)
    return df

def zadanie_4() :
    df = pd.read_sql("""select category.name as category, count(film.title) from film_category
        inner join film on film_category.film_id = film.film_id
        inner join category on film_category.category_id = category.category_id
        group by category.name
    """, con=connection)
    return df

def zadanie_5() :
    df = pd.read_sql("""select customer.first_name, customer.last_name, country.country from customer
    inner join address on customer.address_id = address.address_id
    inner join city on city.city_id = address.city_id
    inner join country on country.country_id = city.country_id
    order by country.country
    """, con=connection)
    return df

def zadanie_6() :
    df = pd.read_sql("""select * from store where store_id in
    (select customer.store_id from customer
    group by customer.store_id
    having count(customer.store_id) < 300 and count(customer.store_id) > 100)
    """, con=connection)
    return df

def zadanie_7() :
    df = pd.read_sql("""select customer.first_name, customer.last_name, sum(film.length) as sum from film
    inner join inventory on film.film_id = inventory.film_id
    inner join rental on rental.inventory_id = inventory.inventory_id
    inner join customer on rental.customer_id = customer.customer_id
    group by customer.first_name, customer.last_name
    having sum(film.length) > 200
    order by sum
    """, con=connection)
    return df

def zadanie_8() :
    df = pd.read_sql("""select film.title, avg(payment.amount) as average_amount from payment
    inner join rental on payment.rental_id = rental.rental_id
    inner join inventory on rental.inventory_id = inventory.inventory_id
    inner join film on inventory.film_id = film.film_id
    group by film.title
    """, con=connection)
    return df

def zadanie_9() :
    df = pd.read_sql("""select category.name as category, avg(film.length) as average_length from film_category
    inner join film on film_category.film_id = film.film_id
    inner join category on film_category.category_id = category.category_id
    group by category.name
    """, con=connection)
    return df

def zadanie_10() :
    df = pd.read_sql("""select category.name as category, max(char_length(film.title)) as max_title_length_per_category from film_category
    inner join film on film_category.film_id = film.film_id
    inner join category on film_category.category_id = category.category_id
    group by category.name
    """, con=connection)
    return df


def zadanie_11() :
    df = pd.read_sql("""select category.name as category, max(film.length) max_length from film_category
    inner join film on film_category.film_id = film.film_id
    inner join category on film_category.category_id = category.category_id
    group by category.name
    """, con=connection)
    return df
 