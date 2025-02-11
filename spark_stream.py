import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO)

CASSANDRA_HOST = 'localhost'
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'users_created'
KEYSPACE = 'spark_streams'
TABLE = 'created_users'


def create_keyspace(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    logging.info("Table created successfully!")

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create Spark session: {e}")
        return None

def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BROKER) \
            .option('subscribe', KAFKA_TOPIC) \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("Connected to Kafka successfully!")
        return df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

def parse_kafka_data(df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    return df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")


def main():
    spark = create_spark_session()
    if not spark:
        return
    
    session = Cluster([CASSANDRA_HOST]).connect()
    create_keyspace(session)
    create_table(session)
    
    kafka_df = connect_to_kafka(spark)
    if kafka_df is None:
        return
    
    parsed_df = parse_kafka_data(kafka_df)
    logging.info("Starting data streaming...")
    
    query = parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .option("keyspace", KEYSPACE) \
        .option("table", TABLE) \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
