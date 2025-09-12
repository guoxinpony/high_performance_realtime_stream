

docker exec -it kafka-broker-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --create \
  --topic financial_transactions \
  --partitions 5 \
  --replication-factor 3
