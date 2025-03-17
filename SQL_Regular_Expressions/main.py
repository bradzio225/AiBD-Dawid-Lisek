import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category) is int:
        df = pd.read_sql(f"""select film.title as title, language.name as languge, category.name as category from film
        inner join film_category on film_category.film_id = film.film_id
        inner join category on film_category.category_id = category.category_id
        inner join language  on language.language_id = film.language_id
        where category.category_id = {category}
        order by film.title, language.name""", con=connection)
        return df
    elif type(category) is str:
        df = pd.read_sql(f"""select film.title as title, language.name as languge, category.name as category from film
        inner join film_category on film_category.film_id = film.film_id
        inner join category on film_category.category_id = category.category_id
        inner join language  on language.language_id = film.language_id
        where category.name like '{category}'
        order by film.title, language.name""", con=connection)
        return df
    else:
        return None

    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category) is int:
        df = pd.read_sql(f"""select film.title as title, language.name as languge, category.name as category from film
        inner join film_category on film_category.film_id = film.film_id
        inner join category on film_category.category_id = category.category_id
        inner join language  on language.language_id = film.language_id
        where category.category_id = {category}
        order by film.title, language.name""", con=connection)
        return df
    elif type(category) is str:
        df = pd.read_sql(f"""select film.title as title, language.name as languge, category.name as category from film
        inner join film_category on film_category.film_id = film.film_id
        inner join category on film_category.category_id = category.category_id
        inner join language  on language.language_id = film.language_id
        where category.name ilike '{category}'
        order by film.title, language.name""", con=connection)
        return df
    else:
        return None
    
def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(title) is str:
        df = pd.read_sql(f"""select actor.first_name, actor.last_name from actor
        inner join film_actor on actor.actor_id = film_actor.actor_id
        inner join film on film_actor.film_id = film.film_id
        where film.title like '{title}'
        order by actor.last_name, actor.first_name
        """, con=connection)
        return df
    else:
        return None
    

def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(words) is list:
        words_re = r'(?:^|\W)(' + r'|'.join(words) + r')(?:$|\W)'
        df = pd.read_sql(f"""select film.title from film
        where film.title ~* '{words_re}'""", connection)
        return df
    else:    
        return None

def zadanie_1():
    df = pd.read_sql("""select country.country from country
    where country.country like 'P%' """, con=connection)
    return df

def zadanie_2():
    df = pd.read_sql("""select country.country from country
    where country.country like 'P%s' """, con=connection)
    return df

def zadanie_3():
    df = pd.read_sql("""select film.title from film
    where film.title like '%[0-9]%' """, con=connection)
    return df

def zadanie_4():
    df = pd.read_sql("""select staff.first_name, staff.last_name from staff
    where staff.first_name like '%-%' or staff.last_name like '%-%'""", connection)
    return df

def zadanie_5():
    df = pd.read_sql("""select actor.last_name from actor
    where actor.last_name ~ '^(P|C)' and CHAR_LENGTH(actor.last_name) = 5""", connection)
    return df

def zadanie_6():
    df = film_title_case_insensitive(['Trip', 'Alone'])
    return df
