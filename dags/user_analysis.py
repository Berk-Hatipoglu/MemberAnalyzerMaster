from datetime import datetime
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)

CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
KEYSPACE = 'spark_streams'
SOURCE_TABLE = 'created_users'

def get_cassandra_session():
    try:
        logging.info("Trying to connect to Cassandra.")
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        logging.info("Successfully connected to Cassandra.")
        return session
    except Exception as e:
        logging.error(f"Cassandra connection error: {e}")
        raise

def ensure_table_exists(session, table_name, create_query):
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{table_name} {create_query}
    """)

def fetch_data():
    session = get_cassandra_session()
    query = f"SELECT id, gender, address, dob FROM {KEYSPACE}.{SOURCE_TABLE};"
    rows = session.execute(query)
    return pd.DataFrame(rows, columns=["id", "gender", "address", "dob"])

def calculate_age(dob):
    today = datetime.today()
    birth_date = datetime.strptime(dob, "%Y-%m-%dT%H:%M:%S.%fZ")
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

def analyze_age():
    try:
        logging.info("Age analysis started.")
        df = fetch_data()
        df["age"] = df["dob"].apply(calculate_age)
        analysis_date = datetime.today().strftime("%Y-%m-%d")
        session = get_cassandra_session()
        
        ensure_table_exists(session, "age", "(age_group TEXT, percentage DOUBLE, analysis_date TEXT, PRIMARY KEY (age_group, analysis_date))")
        
        age_bins = [(0, 15), (16, 30), (31, 45), (46, 60), (60, 150)]
        age_labels = ["0-15", "16-30", "31-45", "46-60", "60+"]
        df["age_group"] = pd.cut(df["age"], bins=[x[0] for x in age_bins] + [age_bins[-1][1]], labels=age_labels, right=True)
        age_distribution = df["age_group"].value_counts(normalize=True) * 100
        for age_group, percentage in age_distribution.items():
            session.execute(f"""
                INSERT INTO {KEYSPACE}.age (age_group, percentage, analysis_date)
                VALUES (%s, %s, %s)
            """, (age_group, percentage, analysis_date))
        logging.info("Age analysis completed successfully.")
    except Exception as e:
        logging.error(f"Error in age analysis: {e}")
        raise

def analyze_gender():
    try:
        logging.info("Gender analysis started.")
        df = fetch_data()
        analysis_date = datetime.today().strftime("%Y-%m-%d")
        session = get_cassandra_session()
        
        ensure_table_exists(session, "gender", "(gender TEXT, percentage DOUBLE, analysis_date TEXT, PRIMARY KEY (gender, analysis_date))")
        
        gender_distribution = df["gender"].value_counts(normalize=True) * 100
        for gender, percentage in gender_distribution.items():
            session.execute(f"""
                INSERT INTO {KEYSPACE}.gender (gender, percentage, analysis_date)
                VALUES (%s, %s, %s)
            """, (gender, percentage, analysis_date))
        logging.info("Gender analysis completed successfully.")
    except Exception as e:
        logging.error(f"Error in gender analysis: {e}")
        raise

def analyze_country():
    try:
        logging.info("Country analysis started.")
        df = fetch_data()
        df["country"] = df["address"].apply(lambda x: x.split(",")[-1].strip())
        analysis_date = datetime.today().strftime("%Y-%m-%d")
        session = get_cassandra_session()
        
        ensure_table_exists(session, "country", "(country TEXT, percentage DOUBLE, analysis_date TEXT, PRIMARY KEY (country, analysis_date))")
        
        country_distribution = df["country"].value_counts(normalize=True) * 100
        for country, percentage in country_distribution.items():
            session.execute(f"""
                INSERT INTO {KEYSPACE}.country (country, percentage, analysis_date)
                VALUES (%s, %s, %s)
            """, (country, percentage, analysis_date))
        logging.info("Country analysis completed successfully.")
    except Exception as e:
        logging.error(f"Error in country analysis: {e}")
        raise

def analyze_city():
    try:
        logging.info("City analysis started.")
        df = fetch_data()
        df["city"] = df["address"].apply(lambda x: x.split(",")[-2].strip())
        analysis_date = datetime.today().strftime("%Y-%m-%d")
        session = get_cassandra_session()
        
        ensure_table_exists(session, "city", "(city TEXT, percentage DOUBLE, analysis_date TEXT, PRIMARY KEY (city, analysis_date))")
        
        city_distribution = df["city"].value_counts(normalize=True) * 100
        for city, percentage in city_distribution.items():
            session.execute(f"""
                INSERT INTO {KEYSPACE}.city (city, percentage, analysis_date)
                VALUES (%s, %s, %s)
            """, (city, percentage, analysis_date))
        logging.info("City analysis completed successfully.")
    except Exception as e:
        logging.error(f"Error in city analysis: {e}")
        raise

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1, 2),
    "retries": 1,
}

with DAG(
    'user_analysis',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False) as dag:

    analyze_age_task = PythonOperator(
        task_id="analyze_age", 
        python_callable=analyze_age
        )
    
    analyze_gender_task = PythonOperator(
        task_id="analyze_gender", 
        python_callable=analyze_gender
        )
    
    analyze_country_task = PythonOperator(
        task_id="analyze_country", 
        python_callable=analyze_country
        )
    
    analyze_city_task = PythonOperator(
        task_id="analyze_city", 
        python_callable=analyze_city
        )
    
    analyze_age_task >> analyze_gender_task >> analyze_city_task >> analyze_country_task
