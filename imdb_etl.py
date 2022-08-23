from bs4 import BeautifulSoup
import requests
import sqlalchemy
import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def scraping():
    try:
        engine = sqlalchemy.create_engine('sqlite:///moviesdb.sqlite', echo=False)
    except:
        print("Database already exists")
    conn = sqlite3.connect('moviesdb.sqlite')
    cur = conn.cursor()

    # Loading data from IMDB
    source = requests.get('https://www.imdb.com/chart/top')
    source.raise_for_status()
    soup = BeautifulSoup(source.text, 'html.parser')

    title = []
    year = []
    rate = []

    # Extracting only relevant data from HTML
    hits_item_lists = soup.find('tbody', class_='lister-list').find_all('tr')
    for elem in hits_item_lists:
        title.append(elem.find(class_='titleColumn').a.text)
        year.append(elem.find(class_='titleColumn').span.text.strip("()"))
        rate.append(elem.find(class_="ratingColumn imdbRating").strong.text)

    # Creating a dictionary mapping
    movies = {
        "title": title,
        "year": year,
        "rating": rate
    }

    movies_df = pd.DataFrame(movies, columns=["title", 'year', 'rating'])
    # Creating tables for data
    sql_query = """
    
    CREATE TABLE IF NOT EXISTS movies(
        rank INTEGER PRIMARY KEY,
        title VARCHAR,
        year INT,
        rating VARCHAR
    )
    """
    cur.execute(""" DROP TABLE IF EXISTS movies """)
    cur.execute(sql_query)
    # Inserting data into databases
    movies_df.to_sql("movies", engine, if_exists='replace' , index=False)
    conn.close()

with DAG(
        default_args=default_args,
        dag_id='scraping',
        description='Scraping data from iMDB',
        start_date=datetime(2022, 6, 6),
        schedule_interval='@weekly'
) as dag:
    scraping_imdb = PythonOperator(
        task_id='scraping_imdb',
        python_callable=scraping
    )

scraping_imdb
