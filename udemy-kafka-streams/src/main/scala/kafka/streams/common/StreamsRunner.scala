package kafka.streams.common

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

object StreamsRunner {
  def propsFor(appName: String): Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.brokers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    props
  }

  def configureFor(appName: String, customProps: Map[String, String] = Map.empty): StreamsConfig = {
    val props = propsFor(appName)
    customProps.foreach(e => props.put(e._1, e._2))
    new StreamsConfig(props)
  }

  def runWith(builder: StreamsBuilder, config: StreamsConfig): Unit = {
    val streams = new KafkaStreams(builder.build(), config)
    streams.cleanUp()
    streams.start()
    println(streams.localThreadsMetadata)

    // required to allow graceful shutdown
    sys.addShutdownHook(streams.close())
  }
}
