import uuid
import json
import logging
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import requests

logging.basicConfig(level=logging.INFO)

KAFKA_BROKER = "broker:29092"
KAFKA_TOPIC = "users_created"

def get_data():
    try:
        logging.info("Trying to request to randomuser.me API")
        res = requests.get("https://randomuser.me/api/")
        res = res.json()
        logging.info(f"Data received from randomuser.me API: {res}")
        return res['results'][0]
    except Exception as e:
        logging.error(f"An error occurred trying to request to randomuser.me API: {e}")
        raise

def format_data(res):
    try:
        logging.info("Trying to formatting the data")
        location = res['location']
        logging.info(f"Data formatted: {res}")
        return {
            'id': str(uuid.uuid4()),
            'first_name': res['name']['first'],
            'last_name': res['name']['last'],
            'gender': res['gender'],
            'address': f"{location['street']['number']} {location['street']['name']}, "
                    f"{location['city']}, {location['state']}, {location['country']}",
            'post_code': location['postcode'],
            'email': res['email'],
            'username': res['login']['username'],
            'dob': res['dob']['date'],
            'registered_date': res['registered']['date'],
            'phone': res['phone'],
            'picture': res['picture']['medium']
            }
    except Exception as e:
        logging.error(f"An error occurred trying to format the data: {e}")
        raise

def stream_data():
    try:
        logging.info("Trying to streaming data to Kafka")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        start_time = time.time()

        while time.time() - start_time < 60:  # Run for 1 minute
            raw_data = get_data()
            formatted_data = format_data(raw_data)
            producer.send(KAFKA_TOPIC, formatted_data)
            logging.info(f"Data sent to Kafka: {formatted_data}")

    except Exception as e:
        logging.error(f"An error occurred while streaming data: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, 1),
    'retries': 1
}

with DAG('user_create',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
