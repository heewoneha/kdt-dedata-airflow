"""
Airflow Day3 Homework
세계 나라 정보 API 사용 DAG 작성
"""

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime

import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url):
    logging.info(datetime.utcnow())
    response = requests.get(url)
    data = response.json()
    return data


@task
def transform(lines):
    records = []
    for l in lines:
        country = l["name"]["official"]
        population = l["population"]
        area = l["area"]
        records.append([country, population, area])
    logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(
            f""" CREATE TABLE IF NOT EXISTS {schema}.{table} (
                country varchar(255),
                population int,
                area float
                );"""
                )
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s)" # 이런 경우 따옴표 처리를 위해 -> Democratic People's Republic...
            cur.execute(sql, tuple(r))
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
    logging.info("load done")


with DAG(
    dag_id = 'CountryInfo',
    start_date = datetime(2023, 6, 7),
    schedule = '30 6 * * 6',  # UTC 매주 토요일 오전 6:30 실행
    catchup = False,
    tags = ['API']
) as dag:

    url = 'https://restcountries.com/v3/all'

    data = transform(extract(url))
    load("jhjmo0719h", "country_info", data)
