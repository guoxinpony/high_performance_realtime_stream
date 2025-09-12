# high_performance_realtime_stream
Implement an ultra-high-performance real-time stream processing platform and end-to-end high-performance data pipeline, including data flow, observability (ELK, Grafana, Prometheus) and system optimization

##### Create kafka topic "financial_transactions":

in powershell/terminal: 

```bash
docker exec -it kafka-broker-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --create \
  --topic financial_transactions \
  --partitions 5 \
  --replication-factor 3
```

in docker container bash:

```bash
/opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:19092 \
--create \
--topic financial_transactions \
--partitions 5 \
--replication-factor 3
```



##### Run Python Producer(test.py: 100,000 msg/s)

```bash
cd kafka-producer-python
python main.py
# python test.py 
```



##### Run Scala Producer(380,000 msg/s)

```bash
cd kafka-producer-scala
sbt "runMain TransactionProducerConcurrentBytes"
```



##### Submit jobs to spark 

in docker bash:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/bitnami/spark/jobs/spark_processor.py
```



in powershell/terminal: 

```bash


docker exec -it high_performance_realtime_stream-spark-master-1 \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/bitnami/spark/jobs/spark_processor.py

```

