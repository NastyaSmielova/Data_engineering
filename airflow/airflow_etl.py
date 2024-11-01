import pandas as pd
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator

from airflow import DAG
from datetime import datetime
from pathlib import Path

def get_data(ti):
    print("Getting data from source")
    my_file = Path("data/AB_NYC_2019.csv")
    if my_file.is_file():
        try:
            df = pd.read_csv(my_file)
            ti.xcom_push(key="raw_data", value=df)
        except:
            print("Errors while reading the file")
            raise ValueError('Errors while reading the file')

    else:
        print("No such file")
        raise ValueError('No such file')


def transform_data(ti):

    raw_data = ti.xcom_pull(key="raw_data", task_ids="get_data")
    transformed_data = raw_data[raw_data['price'] > 0]
    transformed_data.dropna(subset=['latitude', 'longitude'])
    transformed_data['last_review'] = pd.to_datetime(transformed_data['last_review'])
    earliest_date = transformed_data['last_review'].min()

    transformed_data.fillna({'reviews_per_month': 0, 'last_review': earliest_date}, inplace=True)
    transformed_data['name'] = transformed_data['name'].str.replace("'", ' ')
    transformed_data['neighbourhood'] = transformed_data['neighbourhood'].str.replace("'", ' ')

    ti.xcom_push(key="transformed_data", value=transformed_data)


def print_data(ti):
    print(ti.xcom_pull(key="transformed_data", task_ids="transform_data"))


def save_data(ti):
    transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform_data")
    transformed_data.to_csv("data//AB_NYC_2019_transformed.csv")


with DAG(
    "airbnb",
    start_date=datetime(2024, 1, 1),
    max_active_runs=2,
    schedule="@daily",
    catchup=False,
) as dag:
    get_data_task = PythonOperator(
        task_id="get_data", python_callable=get_data
    )

    transform_data_task = PythonOperator(
        task_id="transform_data", python_callable=transform_data
    )

    print_data_task = PythonOperator(
        task_id="print_data", python_callable=print_data
    )

    save_data_task = PythonOperator(
        task_id="save_data", python_callable=save_data
    )

    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgress_localhost",
        sql="""
                CREATE TABLE IF NOT EXISTS airbnb_listings (
                 id SERIAL PRIMARY KEY,
                 name TEXT,
                 host_id INTEGER,
                 host_name TEXT,
                 neighbourhood_group TEXT,
                 neighbourhood TEXT,
                 latitude DECIMAL(9,6),
                 longitude DECIMAL(9,6),
                 room_type TEXT,
                 price INTEGER,
                 minimum_nights INTEGER,
                 number_of_reviews INTEGER,
                 last_review DATE,
                 reviews_per_month DECIMAL(3,2),
                 calculated_host_listings_count INTEGER,
                 availability_365 INTEGER
                );
            """
    )

    get_sql_data_task = PostgresOperator(
        task_id="get_data_postgresql",
        postgres_conn_id="postgress_localhost",
        sql=""" SELECT * FROM airbnb_listings; """
    )

    load_data = PostgresOperator(task_id="insert_data_postgresql",
     postgres_conn_id="postgress_localhost",
     sql=[f"""INSERT INTO airbnb_listings  ( name, host_id, host_name,
                   neighbourhood_group, neighbourhood, latitude, longitude,
                   room_type, price, minimum_nights, number_of_reviews,
                   last_review, reviews_per_month, calculated_host_listings_count,
                   availability_365) VALUES
             {{{{(ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['name'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['host_id'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['host_name'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['neighbourhood_group'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['neighbourhood'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['latitude'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['longitude'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['room_type'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['price'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['minimum_nights'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['number_of_reviews'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['last_review'].strftime('%Y-%m-%d'),
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['reviews_per_month'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['calculated_host_listings_count'],
             ti.xcom_pull(key='transformed_data', task_ids='transform_data').iloc[{i}]['availability_365'])}}}}; """
          for i in range(10)]
     )

    get_data_task >> transform_data_task >> print_data_task >> save_data_task
    save_data_task >> create_table_task >> load_data >> get_sql_data_task
