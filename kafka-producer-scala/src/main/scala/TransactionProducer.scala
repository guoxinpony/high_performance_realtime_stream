import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

object TransactionProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092,localhost:49092") // 你的 Kafka broker 地址
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "1") // ack 机制：1 表示 leader 写入即确认

    val producer = new KafkaProducer[String, String](props)

    try {
      for (i <- 1 to 10) {
        val record = new ProducerRecord[String, String]("test-topic", s"key-$i", s"hello kafka $i")
        producer.send(record)
        println(s"Sent: key-$i -> hello kafka $i")
      }
    } finally {
      producer.flush()
      producer.close()
    }
  }
}
