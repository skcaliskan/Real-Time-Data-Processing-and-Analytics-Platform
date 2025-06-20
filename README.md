# Real-Time-Data-Processing-and-Analytics-Platform

The aim of this project is to design and develop a scalable, efficient, and reliable platform capable of ingesting, processing, analyzing, and visualizing high-volume data streams in real time. Using the technologies learned during the Data Engineering & Big Data Bootcamp, an end-to-end real-time data processing and analytics solution will be implemented. This platform is intended to support timely decision-making by generating actionable insights across various domains.


In this project, real-time electricity data is collected from the Electricity Maps API (https://portal.electricitymaps.com/docs/api#power-breakdown-latest). This API provides up-to-date information about the power breakdown and carbon intensity of electricity consumption by country or region. The retrieved data is then streamed into the platform for further processing, analysis, and visualization. This integration enables real-time monitoring of energy usage patterns and supports the generation of meaningful insights related to sustainability and energy efficiency.

**Project Stages**
1.	Environment Setup on Google Cloud Platform
2.	Data Source and Collection:
    •	 Data Source: “https://portal.electricitymaps.com/docs/api#power-breakdown-latest” this endpoint retrieves data about the origin of electricity in Germany, France, Italy and Spain.
    •	Data Flow: Real-time data flow using Apache Kafka.
3.	Data Processing:
    •	Real-Time Processing: Instant data processing using Apache Spark Streaming.
    •	Batch Processing: Historical data analysis using Apache Spark.
4.	Data Storage:
    •	NoSQL Database: Storing processed data using Apache Cassandra.
    •	Data Warehouse: Creating a data warehouse with Google BigQuery.
5.	Data Analysis and Visualization:
    •	Analytics: Running queries on data with Apache Spark SQL, BigQuery.
    •	Visualization: Creating dashboards using Grafana.
6.	Automation and Orchestration:
    •	Data Pipelines: Automating ETL processes with Apache Airflow.
7.	Deployment and DevOps:
    •	Containerization: Containerizing applications with Docker.
  	
**Technologies to be used**

    •	Programming Language: Python
    
    •	Data Flow: Apache Kafka
    
    •	Data Processing: Apache Spark
    
    •	Data Storage: Apache Cassandra, Parquet
    
    •	Data Warehouse: Google BigQuery
    
    •	Visualization: Grafana
    
    •	Orchestration: Apache Airflow
    
    •	Containerization: Docker GCP
    


**1-IAM permissions have been created.**

•	Compute engine default service account:

•	Editor

•	Owner

•	Dataproc Administrator

•	Storage Admin

•	BirgQuery Admin



**2-VPC firewall rules have been created.** 

•	Ingress: 2181, 3000, 5432, 8080, 8081, 9042, 9043, 9044, 9092

•	Egress ports: 2181, 3000, 5432, 8080, 8081, 9042, 9043, 9044, 9092



**3-Dataproc cluster has been created.**

•	Set up cluster: Region:us-central1, Zone:Any; Cluster Type: Single Node)

•	Configure Nodes: Series:N4, machine type: n4-standat-8, primary disc size: 100 GB

•	Customize cluster: Unselect InternalIp Only


**4- VM Instance has been created by Dataproc**


**5- Update& Uprade and install docker and docker-compose**

```
sudo su

apt update && apt upgrade -y &#& apt install docker.io -y && apt install docker-compose -y
```

**6- nano docker-compose.yml and docker-compose up -d**

```
version: '3.9'
services:
  cassandra-1:
    image: cassandra
    container_name: cassandra1
    hostname: cassandra1
    ports:
      - "9042:9042"
    networks:
      - proje_net
    environment: 
      CASSANDRA_SEEDS: "cassandra1,cassandra2"
      CASSANDRA_CLUSTER_NAME: MyTestCluster 
      CASSANDRA_DC: DC1 
      CASSANDRA_RACK: RACK1 
      CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch 
      CASSANDRA_NUM_TOKENS: 128 
  
  cassandra-2:
    image: cassandra
    container_name: cassandra2
    hostname: cassandra2
    ports:
      - "9043:9042"
    networks:
      - proje_net
    environment: 
      CASSANDRA_SEEDS: "cassandra1,cassandra2"
      CASSANDRA_CLUSTER_NAME: MyTestCluster 
      CASSANDRA_DC: DC1 
      CASSANDRA_RACK: RACK1 
      CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch 
      CASSANDRA_NUM_TOKENS: 128
    
    depends_on:
      cassandra-1:
        condition: service_started
    
  cassandra-3:
    image: cassandra
    container_name: cassandra3
    hostname: cassandra3
    ports:
      - "9044:9042"
    networks:
      - proje_net
    environment: 
      CASSANDRA_SEEDS: "cassandra1,cassandra2"   
      CASSANDRA_CLUSTER_NAME: MyTestCluster 
      CASSANDRA_DC: DC1 
      CASSANDRA_RACK: RACK1 
      CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch 
      CASSANDRA_NUM_TOKENS: 128

    depends_on:
      cassandra-2:
        condition: service_started
    
  graphana:
    image: grafana/grafana-enterprise
    container_name: grafana
    ports:
      - "3000:3000"
    networks:
      - proje_net

  zookeeper:
    image: zookeeper:3.8.0
    container_name: zookeeper-docker
    hostname: zookeeper
    ports:
      - "2181:2181"
    networks:
      - proje_net

  kafka-server-1:
    image: "bitnami/kafka:3.3.1"
    container_name: kafka-container-1
    hostname: kafka-1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 0
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,PLAINTEXT_HOST://:9093"  
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://IP_Adress:9092,PLAINTEXT_HOST://kafka-1:9093" 
      KAFKA_CFG_ZOOKEEPER_CONNECT: zookeeper:2181/kafka-1
      ALLOW_PLAINTEXT_LISTENER: "yes"
    networks:
      - proje_net
      
  kafka-ui:
    container_name: kafka-ui
    image: provectuslabs/kafka-ui:master
    ports:
      - 8080:8080
    depends_on:
      - kafka-server-1
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-1:9093
      DYNAMIC_CONFIG_ENABLED: 'true'
    networks:
      - proje_net
      
  postgres:
    image: postgres:13
    container_name: pg_airflow
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    networks:
      - proje_net

  airflow-webserver:
    image: apache/airflow:2.7.1
    container_name: airflow_web
    restart: always
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@pg_airflow/airflow
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    ports:
      - "8081:8080"
    command: webserver
    networks:
      - proje_net
	  
  airflow-scheduler:
    image: apache/airflow:2.7.1
    container_name: airflow_scheduler
    restart: always
    depends_on:
      - airflow-webserver
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@pg_airflow/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    command: scheduler
    networks:
      - proje_net

volumes:
  cassandra:
  pgdata:
networks:
  proje_net:
    driver: bridge
    
    
```
Services have been controlled with 'docker ps -a'

**7- Kafka topic has been created with Kafka UI**

**8- Data has been transfered from API to Kafka**

```
[
    {
      "id": "1",
	  "headers": {"zone": "DE" },
      "params": {"auth-token": "auth-token1"}
    },
    {
      "id": "2",
      "headers": {"zone": "FR"},
      "params": {"auth-token": "auth-token2"}
    },
    {
      "id": "3",
      "headers": {"zone": "ES"},
      "params": {"auth-token": "auth-token3"}
    },
    {
      "id": "4",
      "headers": {"zone": "IT"},
      "params": {"auth-token": "auth-token4"}
    }
  ]
```
**9- Kafka topic's messages has been controlled from Kafka UI**

**10- Batch data transfer to parquet**

**11- Transfer data from Kafka to Cassandra with Spark in SSH**

**12- Hadoop running applications have been controlled from UI**

**13- Select data from cassandra**

**14- Transfer data from Cassandra To BigQuery**

**15- Connected Cassandra to Grafana and created a dashboard**

**16- Add Apache AirFlow container and create dags**
Two separate DAGs were initially created: one for transferring data from the API to Kafka (transfer_data_from_api_to_kafka), and another for transferring data from Cassandra to BigQuery (transfer_data_from_cassandra_to_bigquery).
Both DAGs were running successfully.
Later, they were combined into a single DAG named etl_data_api_kafkaandcassandra_bigdata.


 
