
# Data Engineering End-to-End Project — Spark, Kafka, Airflow, Docker, Cassandra, Python
Dogukan Ulu
Dogukan Ulu

* https://medium.com/@dogukannulu/data-engineering-end-to-end-project-1-7a7be2a3671

GitHub - dogukannulu/kafka_spark_structured_streaming: Get data from API, run a scheduled script…
Get data from API, run a scheduled script with Airflow, send data to Kafka and consume with Spark, then write to…
github.com

First of all, please visit my repo to be able to understand the whole process better. This project will illustrate a streaming data pipeline and also includes many modern Data tech stack. I also want to mention that I used MacOS for this project.

## Tech Stack

Python
API
Apache Airflow
Apache Kafka
Apache Spark
Apache Cassandra
Docker

## Introduction

We will use Random Name API to get the data. It generates new random data every time we trigger the API. We will get the data using our first Python script. We will run this script regularly to illustrate the streaming data. This script will also write the API data to the Kafka topic. We will also schedule and orchestrate this process using the Airflow DAG script. Once the data is written to the Kafka producer, we can get the data via Spark Structured Streaming script. Then, we will write the modified data to Cassandra using the same script. All the services will be running as Docker containers.

Apache Airflow
We will use Puckel’s Docker-Airflow repo to run the Airflow as a container. Special thanks to Puckel!

We should first run the following command to clone the necessary repo on our local machine.

```
git clone https://github.com/dogukannulu/docker-airflow.git
```

After cloning the repo, we should run the following command to adjust the dependencies.

```
docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .
```

I’ve modified the docker-compose-LocalExecutor.yml file and added it to the repo as well. With this modified version, we will bind the Airflow container with Kafka and Spark containers, and necessary modules and libraries will automatically be installed. Hence, we should add requirements.txt file in the working directory. We can start the container with the following command.

```
docker-compose -f docker-compose-LocalExecutor.yml up -d
```

Now you have a running Airflow container and you can access the UI at https://localhost:8080. If some error occurs with the libraries and packages, we can go into the Airflow container and install it all manually.
```
docker exec -it <airflow_container_name> /bin/bash
curl -O <https://bootstrap.pypa.io/get-pip.py>
sudo yum install -y python3 python3-devel
python3 get-pip.py --user
pip3 install <list all necessary libraries here>
```

Apache Kafka

docker-compose.yml will create a multinode (3) Kafka cluster.
```
version: '3'

services:
  zoo1:
    image: confluentinc/cp-zookeeper:7.3.2
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 1
      ZOOKEEPER_SERVERS: zoo1:2888:3888
    networks:
      - kafka-network
      - airflow-kafka

  kafka1:
    image: confluentinc/cp-kafka:7.3.2
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka1:19092,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9092,DOCKER://host.docker.internal:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_ZOOKEEPER_CONNECT: "zoo1:2181"
      KAFKA_BROKER_ID: 1
      KAFKA_LOG4J_LOGGERS: "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO"
      KAFKA_AUTHORIZER_CLASS_NAME: kafka.security.authorizer.AclAuthorizer
      KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "true"
    networks:
      - kafka-network
      - airflow-kafka

  kafka2:
    image: confluentinc/cp-kafka:7.3.2
    ports:
      - "9093:9093"
      - "29093:29093"
    environment:
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka2:19093,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9093,DOCKER://host.docker.internal:29093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_ZOOKEEPER_CONNECT: "zoo1:2181"
      KAFKA_BROKER_ID: 2
      KAFKA_LOG4J_LOGGERS: "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO"
      KAFKA_AUTHORIZER_CLASS_NAME: kafka.security.authorizer.AclAuthorizer
      KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "true"
    networks:
      - kafka-network
      - airflow-kafka

  kafka3:
    image: confluentinc/cp-kafka:7.3.2
    ports:
      - "9094:9094"
      - "29094:29094"
    environment:
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka3:19094,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9094,DOCKER://host.docker.internal:29094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_ZOOKEEPER_CONNECT: "zoo1:2181"
      KAFKA_BROKER_ID: 3
      KAFKA_LOG4J_LOGGERS: "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO"
      KAFKA_AUTHORIZER_CLASS_NAME: kafka.security.authorizer.AclAuthorizer
      KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "true"
    networks:
      - kafka-network
      - airflow-kafka

  kafka-connect:
    image: confluentinc/cp-kafka-connect:7.3.2
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka1:19092,kafka2:19093,kafka3:19094
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: compose-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: compose-connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: compose-connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: compose-connect-status
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_INTERNAL_KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      CONNECT_INTERNAL_VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      CONNECT_REST_ADVERTISED_HOST_NAME: 'kafka-connect'
      CONNECT_LOG4J_ROOT_LOGLEVEL: 'INFO'
      CONNECT_LOG4J_LOGGERS: 'org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR'
      CONNECT_PLUGIN_PATH: '/usr/share/java,/usr/share/confluent-hub-components'
    networks:
      - kafka-network
      - airflow-kafka

  schema-registry:
    image: confluentinc/cp-schema-registry:7.3.2
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka1:19092,kafka2:19093,kafka3:19094
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - kafka-network
      - airflow-kafka

  kafka-ui:
    container_name: kafka-ui
    image: provectuslabs/kafka-ui:latest
    ports:
      - 8888:8080 # Changed to avoid port clash with akhq
    depends_on:
      - kafka1
      - kafka2
      - kafka3
      - schema-registry
      - kafka-connect
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: PLAINTEXT://kafka1:19092,PLAINTEXT_HOST://kafka1:19092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: connect
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://kafka-connect:8083
      DYNAMIC_CONFIG_ENABLED: 'true'
    networks:
      - kafka-network
      - airflow-kafka

```

As we can see in the file, it has all the necessary services: Kafka, Zookeeper, Kafka-Connect, Schema-Registry, and Kafka-UI.

The docker-compose file also has the services Spark and Cassandra, but we will be mentioning them later. The main point in the Docker container is that we should connect the containers with networks. If not, our scripts cannot write to the Kafka topics or Spark script cannot read the data from the Kafka topic. Also please be careful with the port numbers and check if it is used for other services.

First of all, we should have all these services up and running. Therefore, we should have Docker installed and running. If so, we should only run the following command.

```
docker-compose up -d
```
After running the command and waiting for a bit, we can access Kafka-UI on https://localhost:8888. After the access, we can see Topics on the left menu. We can create a new topic with the name random_names. We should choose the replication factor as 3 since we have 3 Kafka clusters up and running. After triggering the first script, we will be able to see the incoming data as below.


## Kafka-UI
Depending on the first script, data will differ. According to our API data getting script, we can see the messages coming to the Kafka topic as below.


Sample data
Sending the Data to Kafka Topic
Even though the first Python script will be running as Airflow DAG in the end, I would like to introduce the script at this point. We should retrieve data from the Random Names API and write the data to the Kafka topic.

First of all, we have to import necessary libraries and define the logger so that we will keep track of logs better.

```python
import requests
import json
import time
from kafka import KafkaProducer
The following part will create the initial dictionary and modify that.

def create_response_dict(url: str="https://randomuser.me/api/?results=1") -> dict:
    """
    Creates the results JSON from the random user API call
    """
    response = requests.get(url)
    data = response.json()
    results = data["results"][0]

    return results


def create_final_json(results: dict) -> dict:
    """
    Creates the final JSON to be sent to Kafka topic only with necessary keys
    """
    kafka_data = {}

    kafka_data["full_name"] = f"{results['name']['title']}. {results['name']['first']} {results['name']['last']}"
    kafka_data["gender"] = results["gender"]
    kafka_data["location"] = f"{results['location']['street']['number']}, {results['location']['street']['name']}"
    kafka_data["city"] = results['location']['city']
    kafka_data["country"] = results['location']['country']
    kafka_data["postcode"] = int(results['location']['postcode'])
    kafka_data["latitude"] = float(results['location']['coordinates']['latitude'])
    kafka_data["longitude"] = float(results['location']['coordinates']['longitude'])
    kafka_data["email"] = results["email"]

    return kafka_data
```
And this method will create the Kafka topic and will send the data to that regularly.

```
def create_kafka_producer():
    """
    Creates the Kafka producer object
    """

    return KafkaProducer(bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'])


def start_streaming():
    """
    Writes the API data every 10 seconds to Kafka topic random_names
    """
    producer = create_kafka_producer()
    results = create_response_dict()
    kafka_data = create_final_json(results)    

    end_time = time.time() + 120 # the script will run for 2 minutes
    while True:
        if time.time() > end_time:
            break

        producer.send("random_names", json.dumps(kafka_data).encode('utf-8'))
        time.sleep(10)
```

We now have retrieved the data coming from the API, created the Kafka topic, and sent the data to that topic.

## Apache Cassandra
docker-compose.yml will also create a Cassandra server. Every environment variable is located in docker-compose.yml. I also defined them in the scripts.

```
  cassandra:
    image: cassandra:latest
    container_name: cassandra
    hostname: cassandra
    ports:
      - 9042:9042
    environment:
      - MAX_HEAP_SIZE=512M
      - HEAP_NEWSIZE=100M
      - CASSANDRA_USERNAME=cassandra
      - CASSANDRA_PASSWORD=cassandra
    volumes:
      - ./:/home
      - cassandra-data:/var/lib/cassandra
    networks:
      - airflow-kafka
      - kafka-network
```

By running the following command, we can access to Cassandra server (inside the container).

```
docker exec -it cassandra /bin/bash
```

After accessing the bash, we can run the following command to access cqlsh CLI with the necessary user name and password variables.
```
cqlsh -u cassandra -p cassandra
```

Then, we can run the following commands to create the key-space spark_streaming and the table random_names. These will be used for Spark Structured streaming later.
```
CREATE KEYSPACE spark_streaming WITH replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE spark_streaming.random_names(full_name text primary key, gender text, location text, city text, country text, postcode int, latitude float, longitude float, email text);
DESCRIBE spark_streaming.random_names;
```

## Running the commands at cqlsh CLI
Running DAGs
We should move stream_to_kafka.py and stream_to_kafka_dag.py scripts under dags folder in docker-airflow repo to be able to run the DAGs. Then we can see that random_people_names appear on the DAGS page. You may see the full DAG script below.
```
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from stream_to_kafka import start_streaming

start_date = datetime(2018, 12, 21, 12, 12)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('random_people_names', default_args=default_args, schedule_interval='0 1 * * *', catchup=False) as dag:


    data_stream_task = PythonOperator(
    task_id='kafka_data_stream',
    python_callable=start_streaming,
    dag=dag,
    )

    data_stream_task
```
When we turn the OFF button to ON, we can see that the data will be sent to Kafka topics every 10 seconds. We can check the incoming data from the Kafka-UI as well as mentioned above.


DAGs page at localhost:8080
If some error occurs with the libraries and packages, we can go into the Airflow container and install it all manually.
```
docker exec -it <airflow_container_name> /bin/bash
curl -O <https://bootstrap.pypa.io/get-pip.py>
sudo yum install -y python3 python3-devel
python3 get-pip.py --user
pip3 install <list all necessary libraries here>
```
Apache Spark
First of all, I would like to go through all the methods that we will use for Spaark streaming. We first have to import the necessary libraries and define global variables.
```
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,StringType
from pyspark.sql.functions import from_json,col

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")
Then, we have to configure the Spark Session with the necessary jar files.

def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    try:
        # Spark session is established with cassandra and kafka jars. Suitable versions can be found in Maven repository.
        spark = SparkSession \
                .builder \
                .appName("SparkStructuredStreaming") \
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
                .config("spark.cassandra.connection.host", "cassandra") \
                .config("spark.cassandra.connection.port","9042")\
                .config("spark.cassandra.auth.username", "cassandra") \
                .config("spark.cassandra.auth.password", "cassandra") \
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully')
    except Exception:
        logging.error("Couldn't create the spark session")

    return spark
```
After creating the session, we will create an initial data frame and modify it. This will end up with the final data frame.

```
def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly.
    """
    try:
        # Gets the streaming data from topic random_names
        df = spark_session \
              .readStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
              .option("subscribe", "random_names") \
              .option("delimeter",",") \
              .option("startingOffsets", "earliest") \
              .load()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
    """
    Modifies the initial dataframe, and creates the final dataframe.
    """
    schema = StructType([
                StructField("full_name",StringType(),False),
                StructField("gender",StringType(),False),
                StructField("location",StringType(),False),
                StructField("city",StringType(),False),
                StructField("country",StringType(),False),
                StructField("postcode",IntegerType(),False),
                StructField("latitude",FloatType(),False),
                StructField("longitude",FloatType(),False),
                StructField("email",StringType(),False)
            ])

    df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),schema).alias("data")).select("data.*")
    print(df)
    return df
```
Since we obtained the necessary data frame, we can start sending the streaming data to Cassandra.
```
def start_streaming(df):
    """
    Starts the streaming to table spark_streaming.random_names in cassandra
    """
    logging.info("Streaming is being started...")
    my_query = (df.writeStream
                  .format("org.apache.spark.sql.cassandra")
                  .outputMode("append")
                  .options(table="random_names", keyspace="spark_streaming")\
                  .start())

    return my_query.awaitTermination()
```
## Running Spark Script
First of all, we should copy the local PySpark script into the container:
```
docker cp spark_streaming.py spark_master:/opt/bitnami/spark/
```
We should then access the Spark container and install the necessary JAR files under the jars directory. These JAR files are pretty important while writing PySpark scripts to communicate with other services.

docker exec -it spark_master /bin/bash

cd jars
curl -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar
curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.3.0/spark-sql-kafka-0-10_2.13-3.3.0.jar
While the API data is sent to the Kafka topic random_names regularly, we can submit the PySpark application and write the topic data to the Cassandra table with Spark Structured streaming script.

cd ..
spark-submit --master local[2] --jars /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.3.0.jar,/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.3.0.jar spark_streaming.py
After running the command, we can see that the data is populated into the Cassandra table


The logs of the script
Please reach out via Linkedin or Github in case of any questions!
