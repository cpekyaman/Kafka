package kafka.streams.enricher

import kafka.streams.common.StreamsRunner
import org.apache.kafka.streams.kstream.{GlobalKTable, KStream}
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig}

object EnricherApp extends App {
  private def config(): StreamsConfig = StreamsRunner.configureFor(EnricherAppConstants.appName)

  private def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder()

    val usersTable: GlobalKTable[String, String] = streamBuilder.globalTable(EnricherAppConstants.userTable)
    val purchases: KStream[String, String] = streamBuilder.stream(EnricherAppConstants.purchaseStream)

    // join purchase stream with user table
    // map k,v of stream to key of table
    // join two records
    val joined: KStream[String, String] = purchases.join[String, String, String](usersTable, (k, _) => k, (purchase, user) => s"Purchase=$purchase,User=[$user]")
    joined.to("userenricher-purchases-user-join")

    streamBuilder
  }

  StreamsRunner.runWith(builder(), config())
}
