import time, uuid, random, threading, logging
import orjson as json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
TOPIC_NAME = "financial_transactions"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

producer_conf = {
    "bootstrap.servers": KAFKA_BROKERS,
    "linger.ms": 10,                 # 允许微批
    "batch.num.messages": 20000,     # 尝试更大的批量
    "compression.type": "zstd",      # 也可试 lz4/zstd
    "acks": 1,                       # 为吞吐，保持 1
    "queue.buffering.max.messages": 500000,
    "queue.buffering.max.kbytes": 1048576,  # 1GB 缓冲（视内存而定）
    # "delivery.report.only.error": True,
    "broker.address.family": "v4",
}

producer = Producer(producer_conf)

def create_topic():
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS, "broker.address.family": "v4"})
    md = admin.list_topics(timeout=10)
    t = md.topics.get(TOPIC_NAME)
    if t is None or getattr(t, "error", None) is not None:
        fs = admin.create_topics([NewTopic(TOPIC_NAME, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)])
        for _, fut in fs.items():
            fut.result(timeout=20)
        log.info("topic ready")

def gen_txn():
    # 尽量用局部变量捕获，减少全局查找开销
    rn = random.random; ri = random.randint; rc = random.choice; uu = uuid.uuid4
    now = int(time.time())
    return {
        "transactionId": str(uu()),
        "userId": f"user_{ri(1, 100)}",
        "amount": round(50000 + rn() * 100000, 2),
        "transactionTime": now,                         # 如需毫秒: int(time.time()*1000)
        "merchantId": rc(("merchant_1","merchant_2","merchant_3")),
        "transactionType": rc(("purchase","refund")),
        "location": f"location_{ri(1, 50)}",
        "paymentMethod": rc(("credit_card","paypal","bank_transfer")),
        "isInternational": rc((True, False)),          # 不用字符串
        "currency": rc(("USD","EUR","GBP")),
    }

# 统计器
sent_ok = 0
sent_err = 0
stat_lock = threading.Lock()

def on_delivery(err, msg):
    global sent_ok, sent_err
    if err is None:
        with stat_lock:
            sent_ok += 1
    else:
        with stat_lock:
            sent_err += 1

def worker(thread_id: int, stop_event: threading.Event, batch_size: int = 2000):
    
    dumps = json.dumps  # 可替换为 orjson.dumps
    payloads = [dumps(gen_txn()) for _ in range(batch_size)] 
    while not stop_event.is_set():
        # 批量生成，减少 Python 调用开销
        payloads.clear()
        for _ in range(batch_size):
            payloads.append(dumps(gen_txn()))

        # 批量投递
        for v in payloads:
            while True:
                try:
                    producer.produce(TOPIC_NAME, value=v, on_delivery=on_delivery)
                    break
                except BufferError:
                    # 队列满，排空回调并小睡
                    producer.poll(0.01)
        # 触发回调处理，但不 flush
        producer.poll(0)

def start_producers(num_threads: int):
    stop_event = threading.Event()
    threads = [threading.Thread(target=worker, args=(i, stop_event), daemon=True) for i in range(num_threads)]
    for t in threads: t.start()

    # 简单的每秒统计打印 + 定时 flush
    last = time.perf_counter()
    last_ok = last_err = 0
    try:
        while True:
            time.sleep(1)
            producer.poll(0)  # 再兜一遍
            with stat_lock:
                ok, err = sent_ok, sent_err
            now = time.perf_counter()
            r_ok = ok - last_ok
            r_err = err - last_err
            log.info(f"rate={r_ok+r_err:>7d}/s ok={r_ok} err={r_err} totals ok={ok} err={err}")
            last_ok, last_err, last = ok, err, now
            # 适度 flush 以降低堆积时延（不是每条消息）
            producer.flush(0.001)
    except KeyboardInterrupt:
        stop_event.set()
        log.info("shutting down...")
        producer.flush(10)

if __name__ == "__main__":
    create_topic()
    # 先从 4~8 线程起步，观察 CPU 与 broker 吞吐，再做扩/缩
    start_producers(num_threads=6)
