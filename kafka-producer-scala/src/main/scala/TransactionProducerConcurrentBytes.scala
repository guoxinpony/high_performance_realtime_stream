import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import java.util.Properties
import java.util.concurrent.{CountDownLatch, Executors}
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import java.nio.charset.StandardCharsets
import scala.util.Random
import java.util.UUID
import java.math.{ BigDecimal => JBigDecimal, RoundingMode }

object TransactionProducerConcurrentBytes {
  private val merchants      = Array("merchant_1", "merchant_2", "merchant_3")
  private val txTypes        = Array("purchase", "refund")
  private val paymentMethods = Array("credit_card", "paypal", "bank_transfer")
  private val currencies     = Array("USD", "EUR", "GBP")
  private val boolStr        = Array("True", "False")

  // 与步骤6相同的配置 case class
  final case class ProducerCfg(
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

  final case class LoadCfg(
    threads: Int,
    recordsPerThread: Long,
    keyCardinality: Int,
    useKey: Boolean,
    genBatch: Int,
    logEvery: Long,
    mode: String,
    targetRpsPerThread: Long 
  )

  private def loadConfigs(): (ProducerCfg, LoadCfg) = {
    val root = ConfigFactory.load()
    val p = root.getConfig("producer")
    val l = root.getConfig("load")
    val pc = ProducerCfg(
      bootstrapServers  = p.getString("bootstrap.servers"),
      topic             = p.getString("topic"),
      acks              = p.getString("acks"),
      lingerMs          = p.getInt("linger.ms"),
      batchSize         = p.getInt("batch.size"),
      compressionType   = p.getString("compression.type"),
      bufferMemory      = p.getLong("buffer.memory"),
      maxInFlight       = p.getInt("max.in.flight.requests.per.connection"),
      retries           = p.getInt("retries"),
      deliveryTimeoutMs = p.getInt("delivery.timeout.ms"),
      enableIdempotence = p.getBoolean("enable.idempotence")
    )

    
    val lc = LoadCfg(
      threads          = l.getInt("threads"),
      recordsPerThread = l.getLong("records.per.thread"),
      keyCardinality   = l.getInt("key.cardinality"),
      useKey           = l.getBoolean("use.key"),
      genBatch         = l.getInt("gen.batch"),
      logEvery         = l.getLong("log.every"),
      mode             = l.getString("mode"),                 
      targetRpsPerThread = l.getLong("target.rps.per.thread")
    )
    (pc, lc)
  }

  private def buildProps(cfg: ProducerCfg): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers)
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName) // ★ 用字节序列化
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


  private def jsonBytes(rnd: Random, id: Long, sb: StringBuilder): Array[Byte] = {
    sb.setLength(0)

    val transactionId   = UUID.randomUUID().toString
    val userId          = s"user_${1 + rnd.nextInt(100)}"                  // 1..100
    val merchantId      = merchants(rnd.nextInt(merchants.length))
    // amount: uniform(50000, 150000)，保留两位小数，作为“数值”输出（不加引号）
    val amountNum       = new JBigDecimal(50000.0 + rnd.nextDouble() * 100000.0)
                            .setScale(2, RoundingMode.HALF_UP).toPlainString
    val transactionTime = System.currentTimeMillis() / 1000L               // 秒，和 int(time.time()) 一致
    val transactionType = txTypes(rnd.nextInt(txTypes.length))
    val location        = s"location_${1 + rnd.nextInt(50)}"               // 1..50
    val paymentMethod   = paymentMethods(rnd.nextInt(paymentMethods.length))
    val isInternational = boolStr(rnd.nextInt(2))                          // "True"/"False"（字符串）
    val currency        = currencies(rnd.nextInt(currencies.length))

    sb.append('{')
      .append("\"transactionId\":\"").append(transactionId).append("\",")
      .append("\"userId\":\"").append(userId).append("\",")
      .append("\"amount\":").append(amountNum).append(',')                 
      .append("\"transactionTime\":").append(transactionTime).append(',')
      .append("\"merchantId\":\"").append(merchantId).append("\",")
      .append("\"transactionType\":\"").append(transactionType).append("\",")
      .append("\"location\":\"").append(location).append("\",")
      .append("\"paymentMethod\":\"").append(paymentMethod).append("\",")
      .append("\"isInternational\":\"").append(isInternational).append("\",") 
      .append("\"currency\":\"").append(currency).append("\"")
      .append('}')

    sb.toString.getBytes(StandardCharsets.UTF_8)
  }

  def main(args: Array[String]): Unit = {
    val (pc, lc) = loadConfigs()
    val props    = buildProps(pc)
    val topic    = pc.topic

    val exec   = Executors.newFixedThreadPool(lc.threads)
    val latch  = new CountDownLatch(lc.threads)
    val sent   = new AtomicLong(0L)
    val start  = System.nanoTime()

    val cb: Callback = (md: RecordMetadata, ex: Exception) => {
      if (ex != null) System.err.println(s"[ERROR] send failed: ${ex.getMessage}")
    }

    sys.addShutdownHook {
      exec.shutdownNow()
    }

    for (t <- 0 until lc.threads) {
      exec.submit(new Runnable {
        override def run(): Unit = {
          val producer = new KafkaProducer[String, Array[Byte]](props)
          val rnd = new Random(t ^ System.nanoTime())
          val total = lc.recordsPerThread
          val batch = lc.genBatch
          val sb    = new StringBuilder(128) // 线程内复用
          var i = 0L
          try {
            var buf: Array[Array[Byte]] = new Array[Array[Byte]](batch)

            while (!Thread.currentThread().isInterrupted) {
              val n = batch

              // 批量生成字节
              var j = 0
              while (j < n) {
                buf(j) = jsonBytes(rnd, i + j + 1, sb)
                j += 1
              }

              // 批量发送
              j = 0
              while (j < n) {
                if (lc.useKey) {
                  val key = s"k-${(i + j) % lc.keyCardinality}"
                  producer.send(new ProducerRecord[String, Array[Byte]](topic, key, buf(j)), cb)
                } else {
                  producer.send(new ProducerRecord[String, Array[Byte]](topic, buf(j)), cb)
                }
                j += 1
              }

              val cur = sent.addAndGet(n)
              if (cur % lc.logEvery == 0) {
                val el = (System.nanoTime() - start) / 1e9
                val rate = (cur / el).toLong
                println(f"Sent=$cur%,d  Elapsed=${el}%.1fs  ~Rate=$rate%,d msg/s")
              }

              i += n
            }

            producer.flush()
          } finally {
            producer.close(Duration.ofSeconds(10))
            latch.countDown()
          }
        }
      })
    }

    latch.await()
    val elapsed = (System.nanoTime() - start) / 1e9
    val total = sent.get()
    val rate = (total / elapsed).toLong
    println(f"Done. Sent=$total%,d  Elapsed=${elapsed}%.1fs  ~Rate=$rate%,d msg/s")
  }
}
