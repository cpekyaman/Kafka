package kafka.streams.favcolor

import kafka.streams.common.StreamsRunner
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced}
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, StreamsConfig}

object FavouriteColorsApp extends App {
  private def config(): StreamsConfig = StreamsRunner.configureFor("kafka-streams-favcolor")

  private def mapper(key: String, value: String): KeyValue[String, String] = {
    val pair = value.split(",")
    KeyValue.pair(pair(0).toLowerCase, pair(1).toLowerCase)
  }

  private val colors = Set("red", "green", "blue")

  private def colorFilter(user: String, color: String): Boolean = colors.contains(color)

  private def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder()
    val stream: KStream[String, String] = streamBuilder.stream[String, String]("streams-favcolor-input")

    val usersAndColors = stream
      .filter((_, v) => v.indexOf(',') >= 0)
      .map[String, String](mapper)
      .filter(colorFilter)

    usersAndColors.to("streams-favcolor-usercolors")

    val ktab: KTable[String, java.lang.Long] = streamBuilder
      .table("streams-favcolor-usercolors")
      .groupBy((_: String, color: String) => KeyValue.pair(color, color))
      .count()

    ktab.toStream.to("streams-favcolor-output", Produced.`with`(Serdes.String(), Serdes.Long()))

    streamBuilder
  }

  StreamsRunner.runWith(builder(), config())
}
