import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import scala.util.Random
import java.util.concurrent.CountDownLatch
import java.time.Duration



object TransactionProducerHigh {

  case class Config(
    bootstrapServers: String,
    topic: String,
    acks: String,
    lingerMs: Int,
    batchSize: Int,
    compressionType: String,
    bufferMemory: Long,
    maxInFlight: Int,
    retries: Int,
    deliveryTimeoutMs: Int,
    enableIdempotence: Boolean
  )

  private def loadConfig(): Config = {
    val c = ConfigFactory.load().getConfig("producer")
    Config(
      bootstrapServers  = c.getString("bootstrap.servers"),
      topic             = c.getString("topic"),
      acks              = c.getString("acks"),
      lingerMs          = c.getInt("linger.ms"),
      batchSize         = c.getInt("batch.size"),
      compressionType   = c.getString("compression.type"),
      bufferMemory      = c.getLong("buffer.memory"),
      maxInFlight       = c.getInt("max.in.flight.requests.per.connection"),
      retries           = c.getInt("retries"),
      deliveryTimeoutMs = c.getInt("delivery.timeout.ms"),
      enableIdempotence = c.getBoolean("enable.idempotence")
    )
  }

  private def buildProps(cfg: Config): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers)
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.ACKS_CONFIG, cfg.acks)
    p.put(ProducerConfig.LINGER_MS_CONFIG, Int.box(cfg.lingerMs))
    p.put(ProducerConfig.BATCH_SIZE_CONFIG, Int.box(cfg.batchSize))
    p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cfg.compressionType)
    p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.box(cfg.bufferMemory))
    p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Int.box(cfg.maxInFlight))
    p.put(ProducerConfig.RETRIES_CONFIG, Int.box(cfg.retries))
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Int.box(cfg.deliveryTimeoutMs))
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.box(cfg.enableIdempotence))
    p
  }

  /** 生成一条模拟交易 JSON（可替换为你的真实数据） */
  private def genTxn(i: Long): String = {
    val amount = (Random.nextDouble() * 1000).formatted("%.2f")
    val userId = Random.nextInt(100000)
    val merchant = s"m-${Random.nextInt(5000)}"
    s"""{"id":$i,"user_id":$userId,"merchant":"$merchant","amount":$amount,"ts":${System.currentTimeMillis()}}"""
  }

  def main(args: Array[String]): Unit = {
    val cfg   = loadConfig()
    val props = buildProps(cfg)
    val topic = cfg.topic
    val producer = new KafkaProducer[String, String](props)

    // 优雅关闭：Ctrl+C 时 flush + close
    val closed = new CountDownLatch(1)
    sys.addShutdownHook {
      println("Shutting down producer ...")
      try producer.flush() finally {
        producer.close(Duration.ofSeconds(10))
        closed.countDown()
        println("Producer closed.")
      }
    }

    // 异步发送 + 回调（记录成功/失败）
    val total = 100000 // 发送总数（可按需增大）
    val printEvery = 10000

    var sent = 0L
    val start = System.nanoTime()

    for (i <- 1 to total) {
      val key = s"k-${i % 1000}"
      val value = genTxn(i)
      val record = new ProducerRecord[String, String](topic, key, value)

      producer.send(record, new Callback {
        override def onCompletion(md: RecordMetadata, ex: Exception): Unit = {
          if (ex != null) {
            // 这里可以接入日志或指标上报
            System.err.println(s"[ERROR] send failed: ${ex.getMessage}")
          }
        }
      })

      sent += 1
      // 轻量级进度打印，避免刷屏
      if (sent % printEvery == 0) {
        val elapsedSec = (System.nanoTime() - start) / 1e9
        val rate = (sent / elapsedSec).toLong
        println(f"Sent=$sent%,d  Elapsed=${elapsedSec}%.1fs  ~Rate=$rate%,d msg/s")
      }
    }

    // 主动 flush 确保队列清空
    producer.flush()
    val elapsedSec = (System.nanoTime() - start) / 1e9
    val rate = (sent / elapsedSec).toLong
    println(f"Done. Sent=$sent%,d  Elapsed=${elapsedSec}%.1fs  ~Rate=$rate%,d msg/s")

  }
}
