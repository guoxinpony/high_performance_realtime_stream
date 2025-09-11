
# ================================================================
# Python failed.

# using Python to generate data, aiming for a billion records per hour, 277k/s

# in test.py, have implemented 120k/s:

# 2025-09-09 23:48:16,203 INFO rate= 112391/s ok=112391 err=0 totals ok=112391 err=0
# 2025-09-09 23:48:17,335 INFO rate= 115320/s ok=115320 err=0 totals ok=227711 err=0
# 2025-09-09 23:48:18,493 INFO rate= 113736/s ok=113736 err=0 totals ok=341447 err=0
# 2025-09-09 23:48:19,615 INFO rate= 108761/s ok=108761 err=0 totals ok=450208 err=0
# 2025-09-09 23:48:20,875 INFO rate= 130422/s ok=130422 err=0 totals ok=580630 err=0
# 2025-09-09 23:48:21,993 INFO rate= 114770/s ok=114770 err=0 totals ok=695400 err=0
# 2025-09-09 23:48:23,240 INFO rate= 128182/s ok=128182 err=0 totals ok=823582 err=0
# 2025-09-09 23:48:24,503 INFO rate= 128877/s ok=128877 err=0 totals ok=952459 err=0


# =================================================================

import time
import uuid
import random
import json
import threading
import logging
import traceback
from confluent_kafka import KafkaException, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = "financial_transactions"

# --- 强制重置 logging，并同时输出到控制台 + 文件 ---
logging.basicConfig(force=True, level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
root = logging.getLogger()
fh = logging.FileHandler("create_topic.log", mode="w", encoding="utf-8")
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
root.addHandler(fh)

logger = logging.getLogger(__name__)
logger.info("👋 Script started.")  # 起始标记


producer_conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 10000, # limit use of memory
    'queue.buffering.max.kbytes': 512000,  # 512 MB
    'batch.num.messages': 1000,
    'linger.ms': 10,
    'acks': 1,  # ACK: send confirmation after receive a record successfully
    'compression.type': 'gzip'
}

producer = Producer(producer_conf)

# librdkafka 日志回调 -> 接到 Python logging
def rdk_logger(conf, level, fac, buf):
    # level: 0..3（大致对应 DEBUG/INFO/WARN/ERROR）
    py_level = {0: logging.DEBUG, 1: logging.INFO, 2: logging.WARNING, 3: logging.ERROR}.get(level, logging.DEBUG)
    logging.log(py_level, f"[{fac}] {buf}")

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed for record {msg.key()}')
    # else:
    #     print(f'Record {msg.key()} successfully produced')


def create_topic(topic_name: str):
    admin_client = AdminClient({
        "bootstrap.servers": KAFKA_BROKERS,
        "debug": "broker,admin",         
        "log_level": 7,                  
        "logger": rdk_logger,            
        "broker.address.family": "v4",  
    })

    logger.info("📡 Calling list_topics() ...")
    try:
        md = admin_client.list_topics(timeout=10)
        logger.info(f"Cluster id={md.cluster_id}, brokers={len(md.brokers)}")

        t = md.topics.get(topic_name)
        if t is None or getattr(t, "error", None) is not None:
            logger.info(f"⏳ Creating topic: {topic_name} (partitions={NUM_PARTITIONS}, rf={REPLICATION_FACTOR})")
            new_topic = NewTopic(topic=topic_name, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
            fs = admin_client.create_topics([new_topic], request_timeout=15)

            for name, fut in fs.items():
                try:
                    fut.result(timeout=20)
                    logger.info(f"✅ Topic '{name}' created successfully!")
                except KafkaException as ke:
                    err: KafkaError = ke.args[0]
                    logger.error(f"❌ KafkaException creating '{name}': {err.str()} (code={err.code()})")
                    logger.error(traceback.format_exc())
                except Exception as e:
                    logger.error(f"❌ Unexpected error creating '{name}': {e}")
                    logger.error(traceback.format_exc())
        else:
            logger.info(f"ℹ️ Topic '{topic_name}' already exists.")
    except KafkaException as ke:
        err: KafkaError = ke.args[0]
        logger.error(f"❌ Top-level KafkaException: {err.str()} (code={err.code()})")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"❌ Top-level Exception: {e}")
        logger.error(traceback.format_exc())



# generate single record
def generate_transaction():
    return dict(
        transactionId=str(uuid.uuid4()),
        userId=f"user_{random.randint(1, 100)}",
        amount=round(random.uniform(50000, 150000), 2),
        transactionTime=int(time.time()),
        merchantId=random.choice(['merchant_1', 'merchant_2', 'merchant_3']),
        transactionType=random.choice(['purchase', 'refund']),
        location=f'location_{random.randint(1, 50)}',
        paymentMethod=random.choice(['credit_card', 'paypal', 'bank_transfer']),
        isInternational=random.choice(['True', 'False']),
        currency=random.choice(['USD', 'EUR', 'GBP'])
    )


# produce  transaction and send to kafka
def produce_transaction(thread_id):
    while True:
        transaction = generate_transaction()

        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction['userId'],
                value=json.dumps(transaction).encode('utf-8'),
                on_delivery=delivery_report
            )
            # print(f' Thread {thread_id} - Produced transaction: {transaction}')
            # producer.flush()
        except Exception as e:
            print(f'Error sending transaction: {e}')


def producer_data_in_parallel(num_threads):
    threads = []
    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transaction, args=(i,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except Exception as e:
        print(f'Error message {e}')


if __name__ == "__main__":
    # print(">>> RUNNING create_topic ...", flush=True)  # 兜底：即使 logging 失效也会看到
    create_topic(TOPIC_NAME)
    producer_data_in_parallel(3)
    # print(">>> DONE.", flush=True)

