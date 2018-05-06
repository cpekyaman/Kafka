package kafka.streams.common

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

object ProducerBuilder {
  private def propsFor(clientId: String): Properties = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    props
  }

  def buildFor(clientId: String, customProps: Map[String, String] = Map.empty): Producer[String, String] = {
    val props: Properties = propsFor(clientId)
    customProps.foreach(e => props.put(e._1, e._2))

    new KafkaProducer[String, String](props)
  }
}
