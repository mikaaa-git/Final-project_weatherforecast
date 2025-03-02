import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def key_space(session):
    session.execute("""CREATE KEYSPACE IF NOT EXISTS spark_streaming 
                    WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}""")
    logging.info('Keyspace created successfully')

def table(session):
    session.execute("""CREATE TABLE IF NOT EXISTS spark_streaming.airpollution_data(
                    timestamp text PRIMARY KEY,
                    lon text,
                    lat text,
                    AQI text,
                    CO text,
                    NO text,
                    NO2 text,
                    O3 text,
                    SO2 text,
                    PM2_5 text,
                    PM10 text,
                    NH3 text);""")

def insert_data(session, **kwargs):
    logging.info('Inserting data into cassandra')
    try:
        session.execute("""INSERT INTO spark_streaming.airpollution_data(timestamp, lon, lat, AQI, CO, NO, NO2, O3, SO2, PM2_5, PM10, NH3) 
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (kwargs['timestamp'], kwargs['lon'], kwargs['lat'], kwargs['AQI'], kwargs['CO'], kwargs['NO'],
                         kwargs['NO2'], kwargs['O3'], kwargs['SO2'], kwargs['PM2_5'], kwargs['PM10'], kwargs['NH3']))
        logging.info(f'Data inserted successfully example: {kwargs["timestamp"]} {kwargs["AQI"]} {kwargs["lat"]} {kwargs["lon"]}')
    except Exception as e:
        logging.error(f"Error in inserting data due to {e}")

def spark_connection():
    try:
        spark = SparkSession.builder.appName('airpollution')\
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.5") \
            .config('spark.cassandra.connection.host', 'localhost').getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        logging.info('Spark session created successfully')
        return spark
    except Exception as e:
        logging.error(f"Error in spark connection due to {e}")
        return None

def connect_kafka(spark):
    try:
        spark_df = spark.readStream.format('kafka')\
            .option('kafka.bootstrap.servers', 'localhost:9092')\
            .option('subscribe', 'airpollution')\
            .option('startingOffsets', 'earliest')\
            .load()
        logging.info('Connected to kafka successfully')
        return spark_df
    except Exception as e:
        logging.error(f"Error in connecting to kafka due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([StructField('timestamp', StringType(), False),
                         StructField('lon', StringType(), False),
                         StructField('lat', StringType(), False),
                         StructField('AQI', StringType(), False),
                         StructField('CO', StringType(), False),
                         StructField('NO', StringType(), False),
                         StructField('NO2', StringType(), False),
                         StructField('O3', StringType(), False),
                         StructField('SO2', StringType(), False),
                         StructField('PM2_5', StringType(), False),
                         StructField('PM10', StringType(), False),
                         StructField('NH3', StringType(), False)])
    
    return spark_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col('value'), schema).alias('data'))\
        .select('data.*')

def cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connect()
        logging.info('Cassandra connection created successfully')
        return cassandra_session
    except Exception as e:
        logging.error(f"Error in cassandra connection due to {e}")
        return None

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    spark = spark_connection()
    
    if spark is not None:
        spark_df = connect_kafka(spark)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = cassandra_connection()
            if session is not None:
                key_space(session)
                table(session)
                steaming_query = selection_df.writeStream.format("org.apache.spark.sql.cassandra")\
                    .option("checkpointLocation", "/tmp/checkpoint")\
                    .option("keyspace", 'spark_streaming')\
                    .option("table", 'airpollution_data')\
                    .start()
                steaming_query.awaitTermination()