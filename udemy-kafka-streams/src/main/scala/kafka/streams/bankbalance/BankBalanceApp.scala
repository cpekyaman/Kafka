package kafka.streams.bankbalance

import kafka.streams.common.StreamsRunner
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig}

/**
  * Calculate last transaction time and total balance for each customer
  */
object BankBalanceApp extends App {
  private def config(): StreamsConfig = StreamsRunner.configureFor(BankBalanceAppConstants.appName, Map(StreamsConfig.PROCESSING_GUARANTEE_CONFIG -> StreamsConfig.EXACTLY_ONCE))

  private def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder()
    val stream: KStream[String, String] = streamBuilder.stream[String, String](BankBalanceAppConstants.inputTopic)

    //TODO:
    // 1. parse json with mapper (jackson databind) or use json serde (kafka connect)
    // 2. create an initial object (JsonNode or similar) to hold time and initial balance
    // 3. group by key
    // 4. update initials object with aqgregate (sum for balance & max compare for time)
    // 5. write the table to another stream (log compacted topic because of ktable)
    //stream.groupByKey().aggregate()

    streamBuilder
  }

  StreamsRunner.runWith(builder(), config())
}
