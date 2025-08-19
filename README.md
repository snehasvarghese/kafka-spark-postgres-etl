# Kafka-Spark-Postgres ETL Pipeline  

##  Overview  
This project demonstrates a real-time **ETL (Extract, Transform, Load) pipeline** using:  
- **Apache Kafka** for message streaming  
- **Apache Spark Structured Streaming** for data processing  
- **PostgreSQL** as the storage layer  

The pipeline ingests sample drug data from a Kafka Producer, processes it in Spark, and writes the results into PostgreSQL.  

##  Architecture  

**Workflow:**  
1. **Producer** sends data (`drugs.csv`) into a Kafka topic.  
2. **Spark Consumer** subscribes to the Kafka topic and performs transformations.  
3. **Processed data** is written into PostgreSQL for storage and analysis.  

##  Tech Stack  
- **Python 3.12**  
- **Apache Kafka**  
- **Apache Spark 3.x (Structured Streaming)**  
- **PostgreSQL 14+**  

## Setup Instructions  

### 1. Clone the Repository  
```bash
git clone https://github.com/snehasvarghese/kafka-spark-postgres-etl.git
cd kafka-spark-postgres-etl
```
### 2. Create Virtual Environment & Install Dependencies
```bash
python3 -m venv venv
source venv/bin/activate
```
### 3. Start Kafka & PostgreSQL
-	Ensure Kafka broker and PostgreSQL server are running.
- Update connection details in `sparkConsumer.py` if needed.

### 4. Run the Producer
```bash
python3 kafkaProducer.py
```
### 5. Run the Spark Consumer
```bash
python3 sparkConsumer.py
```

## Example Output
-	Kafka Producer publishes rows from drugs.csv.
-	Spark Consumer processes the stream and inserts data into PostgreSQL table.
-	You can query the results in Postgres:
```sql
SELECT * FROM drugs LIMIT 10;
```

## Future Improvements
- Dockerize the pipeline for easier deployment
- Extend to multiple Kafka topics and transformations

