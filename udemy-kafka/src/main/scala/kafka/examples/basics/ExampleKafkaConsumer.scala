package kafka.examples.basics

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object ExampleKafkaConsumer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", KafkaConstants.brokers)
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)
  props.put("group.id", "UdemyConsumer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(List(KafkaConstants.topic).asJava)

  while (true) {
    val records = consumer.poll(200).asScala
    records.foreach { r => println(r.toString) }
  }
}
