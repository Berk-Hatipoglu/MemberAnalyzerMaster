from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt
import logging
import os

logging.basicConfig(level=logging.INFO)

CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
KEYSPACE = 'spark_streams'

def fetch_age_data(**kwargs):
    try:
        logging.info("Fetching age data from Cassandra.")
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        date_str = datetime.now().strftime('%Y-%m-%d')
        query = f"SELECT age_group, percentage FROM {KEYSPACE}.age WHERE analysis_date = '{date_str}' ALLOW FILTERING;"
        rows = session.execute(query)
        
        data = {'age_groups': [], 'percentages': []}
        for row in rows:
            data['age_groups'].append(row.age_group)
            data['percentages'].append(row.percentage)
        
        kwargs['ti'].xcom_push(key='age_data', value=data)
        logging.info("Age data fetched and saved to XCom.")
    except Exception as e:
        logging.error(f"An error occurred while fetching age data: {e}")
        raise

def fetch_gender_data(**kwargs):
    try:
        logging.info("Fetching gender data from Cassandra.")
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        date_str = datetime.now().strftime('%Y-%m-%d')
        query = f"SELECT gender, percentage FROM {KEYSPACE}.gender WHERE analysis_date = '{date_str}' ALLOW FILTERING;"
        rows = session.execute(query)
        
        data = {'genders': [], 'percentages': []}
        for row in rows:
            data['genders'].append(row.gender)
            data['percentages'].append(row.percentage)
        
        kwargs['ti'].xcom_push(key='gender_data', value=data)
        logging.info("Gender data fetched and saved to XCom.")
    except Exception as e:
        logging.error(f"An error occurred while fetching gender data: {e}")
        raise

def fetch_country_data(**kwargs):
    try:
        logging.info("Fetching country data from Cassandra.")
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        date_str = datetime.now().strftime('%Y-%m-%d')
        query = f"SELECT country, percentage FROM {KEYSPACE}.country WHERE analysis_date = '{date_str}' ALLOW FILTERING;"
        rows = session.execute(query)
        
        data = {'countries': [], 'percentages': []}
        for row in rows:
            data['countries'].append(row.country)
            data['percentages'].append(row.percentage)
        
        kwargs['ti'].xcom_push(key='country_data', value=data)
        logging.info("Country data fetched and saved to XCom.")
    except Exception as e:
        logging.error(f"An error occurred while fetching country data: {e}")
        raise

def plot_data(data_key, x_label, title, file_name, **kwargs):
    try:
        logging.info(f"Plotting data for {data_key}.")
        data = kwargs['ti'].xcom_pull(key=data_key, task_ids=f'fetch_{data_key.split("_")[0]}_data')
        categories = list(data.keys())[0]
        values = list(data.keys())[1]
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        num_elements = len(data[categories])
        rotation_angle = 90 if num_elements > 20 else 0  # If the number of columns is greater than 20, it is vertical, if it is smaller, it is horizontal.
        
        plt.figure(figsize=(8, 5))
        bars = plt.bar(data[categories], data[values], color='skyblue')
        plt.xlabel(x_label)
        plt.ylabel('Percentage')
        plt.title(f'{title} on {date_str}')
        plt.xticks(rotation=rotation_angle, fontsize=8)
        plt.subplots_adjust(bottom=0.3 if rotation_angle == 90 else 0.15)
        
        for bar, value in zip(bars, data[values]):
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{value:.0f}%', 
                    ha='center', va='bottom', fontsize=8, fontweight='bold', color='black')

        base_directory = '/usr/local/airflow/analyze_graphics'
        if not os.path.exists(base_directory):
            os.makedirs(base_directory)

        directory = os.path.join('/usr/local/airflow/analyze_graphics', date_str)
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        file_path = os.path.join(directory, file_name)
        plt.savefig(file_path)
        plt.close()
        logging.info(f"Data plotted for {data_key} and saved to {file_path}.")
    except Exception as e:
        logging.error(f"An error occurred while plotting data: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1,3),
}

with DAG(
    'generate_graphs',
    default_args=default_args,
    description='Generate multiple graphs from Cassandra data',
    schedule_interval="@daily",
    catchup=False) as dag:

    fetch_age_task = PythonOperator(
        task_id='fetch_age_data',
        python_callable=fetch_age_data,
        dag=dag,
    )

    fetch_gender_task = PythonOperator(
        task_id='fetch_gender_data',
        python_callable=fetch_gender_data,
        dag=dag,
    )

    fetch_country_task = PythonOperator(
        task_id='fetch_country_data',
        python_callable=fetch_country_data,
        dag=dag,
    )

    plot_age_task = PythonOperator(
        task_id='plot_age_data',
        python_callable=plot_data,
        op_kwargs={'data_key': 'age_data', 'x_label': 'Age Group', 'title': 'Percentage Distribution by Age Group', 'file_name': 'age_distribution.png'},
        dag=dag,
    )

    plot_gender_task = PythonOperator(
        task_id='plot_gender_data',
        python_callable=plot_data,
        op_kwargs={'data_key': 'gender_data', 'x_label': 'Gender', 'title': 'Percentage Distribution by Gender', 'file_name': 'gender_distribution.png'},
        dag=dag,
    )

    plot_country_task = PythonOperator(
        task_id='plot_country_data',
        python_callable=plot_data,
        op_kwargs={'data_key': 'country_data', 'x_label': 'Country', 'title': 'Percentage Distribution by Country', 'file_name': 'country_distribution.png'},
        dag=dag,
    )

    fetch_age_task >> plot_age_task
    fetch_gender_task >> plot_gender_task
    fetch_country_task >> plot_country_task
