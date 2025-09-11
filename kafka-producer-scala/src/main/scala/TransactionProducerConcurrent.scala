import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.util.concurrent.{CountDownLatch, Executors}
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import scala.util.Random

object TransactionProducerConcurrent {

  // —— 与步骤5相同的 Producer 配置 —— //
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
    logEvery: Long
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
      logEvery         = l.getLong("log.every")
    )
    (pc, lc)
  }

  private def buildProps(cfg: ProducerCfg): Properties = {
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
  private def genTxn(rnd: Random, id: Long): String = {
    val amount   = (rnd.nextDouble() * 1000).formatted("%.2f")
    val userId   = rnd.nextInt(100000)
    val merchant = s"m-${rnd.nextInt(5000)}"
    s"""{"id":$id,"user_id":$userId,"merchant":"$merchant","amount":$amount,"ts":${System.currentTimeMillis()}}"""
  }

  def main(args: Array[String]): Unit = {
    val (pc, lc) = loadConfigs()
    val props    = buildProps(pc)
    val topic    = pc.topic

    val exec   = Executors.newFixedThreadPool(lc.threads)
    val latch  = new CountDownLatch(lc.threads)
    val sent   = new AtomicLong(0L)
    val start  = System.nanoTime()

    // 统一回调（打印错误即可，避免过多日志）
    val cb: Callback = (md: RecordMetadata, ex: Exception) => {
      if (ex != null) System.err.println(s"[ERROR] send failed: ${ex.getMessage}")
    }

    // 优雅关闭：Ctrl+C
    sys.addShutdownHook {
      println("Shutting down executor ...")
      exec.shutdownNow()
    }

    for (t <- 0 until lc.threads) {
      exec.submit(new Runnable {
        override def run(): Unit = {
          val producer = new KafkaProducer[String, String](props)
          val rnd = new Random(t ^ System.nanoTime())
          val total = lc.recordsPerThread
          val batch = lc.genBatch
          var i = 0L
          try {
            val buf = new Array[String](batch)

            while (i < total) {
              // 批量生成
              val n = Math.min(batch, (total - i).toInt)
              var j = 0
              while (j < n) {
                buf(j) = genTxn(rnd, i + j + 1)
                j += 1
              }

              // 批量发送
              j = 0
              while (j < n) {
                val key =
                  if (lc.useKey) s"k-${(i + j) % lc.keyCardinality}"
                  else null
                val record =
                  if (key == null) new ProducerRecord[String, String](topic, buf(j))
                  else new ProducerRecord[String, String](topic, key, buf(j))
                producer.send(record, cb)
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
