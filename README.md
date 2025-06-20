****Real-Time-Data-Processing-and-Analytics-Platform****

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

Programming Language: Python
    
Data Flow: Apache Kafka
    
Data Processing: Apache Spark
    
Data Storage: Apache Cassandra, Parquet
    
Data Warehouse: Google BigQuery
    
Visualization: Grafana
    
Orchestration: Apache Airflow
    
Containerization: Docker GCP
    
**Environment Setup on Google Cloud Platform**

In this project, the environment setup began with the configuration of **Identity and Access Management (IAM)** permissions to control access to cloud resources securely. 
Compute engine default service account:

•	Editor

•	Owner

•	Dataproc Administrator

•	Storage Admin

•	BirgQuery Admin


And then **Virtual Private Cloud (VPC)** networks appropriate firewall rules were defined to manage network traffic and ensure secure communication between services. 

Ingress ports: 2181, 3000, 5432, 8080, 8081, 9042, 9043, 9044, 9092

Egress ports: 2181, 3000, 5432, 8080, 8081, 9042, 9043, 9044, 9092


Once the security and networking setup was completed, a cluster for real-time data processing was provisioned using **Google Cloud Dataproc.** The cluster was configured with appropriate machine types and sufficient storage to efficiently handle data. 

•	Set up cluster: Region:us-central1, Zone:Any; Cluster Type: Single Node)

•	Configure Nodes: Series:N4, machine type: n4-standat-8, primary disc size: 100 GB

•	Customize cluster: Unselect InternalIp Only


**The necessary installations and upgrades** are performed by connecting to the VM machine via SSH.

```
sudo su

apt update && apt upgrade -y &#& apt install docker.io -y && apt install docker-compose -y
```

“nano docker-compose.yml” A **docker-compose.yml** file is created for the technologies to be used in the project.
The “docker-compose up” command is used to start and run multiple Docker containers defined in the docker-compose.yml file. This command simplifies the process of launching multi-container applications by handling networking, volume mounting, and dependency management automatically.

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

**Data Source**

“https://portal.electricitymaps.com/docs/api#power-breakdown-latest” this endpoint retrieves data about the origin of electricity in an area. Germany, France, Italy and Spain have been used for this project.
Information provided by the API:
•	"powerProduction" (in MW) represents the electricity produced in the zone, broken down by production type
•	"powerConsumption" (in MW) represents the electricity consumed in the zone, after taking into account imports and exports, and broken down by production type.
•	"powerExport" and "Power import" (in MW) represent the physical electricity flows at the zone border
•	"renewablePercentage" and "fossilFreePercentage" refers to the % of the power consumption breakdown coming from renewables or fossil-free power plants (renewables and nuclear). It can either be queried by zone identifier or by geolocation.
Data Collection: Real-time data flow using Apache Kafka
Apache Kafka is a distributed event streaming platform designed for high-throughput, fault-tolerant, and real-time data transmission. In this project, Kafka is used to serve as a central messaging backbone. Kafka decouples the data source from downstream systems, allowing other components to consume and process the data independently and efficiently. This approach ensures reliable and scalable data flow across the platform.

Kafka topic has been created with Kafka UI.

For interactive processing and analysis, Jupyter Notebooks with PySpark were used within the Dataproc environment. Real-time electricity data retrieved from the Electricity Maps API is published to a Kafka topic using a custom ingestion script. (FromApiToKafka.ipynb)

Germany, France, Italy and Spain have been used for this project. An tokens.json file includes API’s parameters for these zones:
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
Kafka topic's messages has been controlled from Kafka UI.

**Data Processing & Data Storage**

**Batch Processing**

All messages accumulated in Kafka have been batch transferred to Parquet files. This batch processing approach allows for efficient writing of large volumes of data into the columnar Parquet format, which optimizes storage space and query performance. (FromKafkaToParquetFile.py)

**Real-Time Processing**

Real-time data processing is performed using Apache Spark Structured Streaming, which continuously consumes messages from the Kafka topics. The streaming data is processed, transformed, and enriched on-the-fly, and then written directly to Apache Cassandra. Cassandra’s distributed, high-availability architecture makes it well-suited for handling the fast and large-scale write operations generated by the real-time pipeline. This integration enables efficient storage and retrieval of streaming data for immediate analytics and querying.

Firstly keyspace and table has been create in cassandra, and then spark code executed with SparkSession:

```

CREATE KEYSPACE IF NOT EXISTS power WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};



CREATE TABLE IF NOT EXISTS power.powerbreakdown (
    id UUID PRIMARY KEY,
    zoneid INT,
    zone TEXT, 
    getdatadatetime Timestamp,
    cons_nuclear INT,
    cons_geothermal INT,
    cons_biomass INT,
    cons_coal INT,
    cons_solar INT,
    cons_wind INT,
    cons_hydro INT,
    cons_gas INT,
    cons_oil INT,
    cons_unknown INT,
    prod_nuclear INT,
    prod_geothermal INT,
    prod_biomass INT,
    prod_coal INT,
    prod_solar INT,
    prod_wind INT,
    prod_hydro INT,
    prod_gas INT,
    prod_oil INT,
    prod_unknown INT,
consumption_total INT,
production_total INT,
import_total INT,
export_total INT
);
```

The streaming data is processed, transformed, and enriched on-the-fly, and then written directly to Apache Cassandra. (TransferDataFromKafkaToCassandra.ipynb)


**Transfer data from Cassandra To BigQuery**

Data stored in Apache Cassandra is transferred to Google BigQuery for advanced analytics and reporting. BigQuery’s fully managed, serverless data warehouse provides powerful SQL-based querying capabilities and supports large-scale data analysis. By moving data from Cassandra to BigQuery, the project enables efficient, scalable, and fast analytical queries, making it easier to generate business insights and visualizations.(TransferDataFromCassandraToBigQuery.py)

**Data Analysis and Visualization**

Custom queries were developed to generate subtables in **BigQuery** for organizing and optimizing data analysis

To enable effective monitoring and visualization, Grafana—a popular open-source analytics and monitoring platform—is connected to Cassandra. **Grafana** allows the creation of dynamic dashboards that reflect up-to-date energy consumption and production metrics. This integration provides an interactive platform for analyzing trends and making data-driven decisions based on the stored electricity data.

**Automation and Orchestration**

Data ingestion and pipeline orchestration in this project are managed using **Apache Airflow**, an open-source platform designed to programmatically author, schedule, and monitor workflows. Real-time electricity data is extracted from the Electricity Maps API and ingested into Apache Kafka via scheduled workflows in Airflow. Additionally, Airflow orchestrates the batch transfer of processed data from Apache Cassandra to Google BigQuery for advanced analytics and reporting. (api_kafka_cassandra_big_querydag.py)

**Conclusion**

This project successfully demonstrates the design and implementation of a real-time data processing and analytics pipeline using modern big data technologies. By leveraging tools such as Apache Kafka, Spark, Cassandra, and Google Cloud services like Dataproc and BigQuery, a scalable and efficient platform was built to ingest, process, store, and visualize electricity data retrieved from the Electricity Maps API. The integration of Apache Airflow ensured reliable and automated orchestration of data workflows, while Grafana enabled real-time monitoring through dynamic dashboards. Overall, the project showcases a robust end-to-end solution for handling streaming data and generating actionable insights in real time.
All project details are in 

**Acknowledgements**

I would like to express my heartfelt thanks to Zekeriya Beşiroğlu for his valuable guidance and mentorship throughout the bootcamp. I am also grateful to all the participants of the program for their collaboration and support. Special thanks to İstanbul Data Science Academy for organizing this enriching learning experience. Lastly, I extend my deepest gratitude to my beloved family, who supported me unconditionally and gave me the space to focus and grow during this journey.

 
