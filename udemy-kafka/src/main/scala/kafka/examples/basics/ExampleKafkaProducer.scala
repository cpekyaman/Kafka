package kafka.examples.basics

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object ExampleKafkaProducer extends App {

  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.brokers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "UdemyProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "2")

  val producer = new KafkaProducer[String, String](props)

  (0 to 15).foreach { i =>
    producer.send(new ProducerRecord[String, String](KafkaConstants.topic, i.toString, "My message " + i))
  }
  producer.close()

}
