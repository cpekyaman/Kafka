package kafka.streams.wordcount

import java.lang
import java.util.Locale

import kafka.streams.common.StreamsRunner
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced, ValueMapper}
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig}

import scala.collection.JavaConverters._

object WordCountApp extends App {

  private def config(): StreamsConfig = StreamsRunner.configureFor(WordCountAppConstants.appName)

  def builder(): StreamsBuilder = {
    //TODO: after 1.1.0 mapValues and flatMapValues does not compile without these explicit interfaces
    val valueMapper: ValueMapper[String, String] = (v: String) => v.toLowerCase(Locale.ENGLISH)
    val flatValueMapper: ValueMapper[String, java.util.List[String]] = (v: String) => v.split(" ").toList.asJava

    val streamBuilder = new StreamsBuilder()
    val stream: KStream[String, String] = streamBuilder.stream[String, String](WordCountAppConstants.inputStream)

    // read from kafka
    // to lower case words in message
    // map a message to a list of words
    // use word itself as key
    // group by key (word) and count
    val ktab: KTable[String, lang.Long] = stream
      .mapValues[String](valueMapper)
      .flatMapValues[String](flatValueMapper)
      .selectKey[String]((_, v) => v)
      .groupByKey()
      .count()

    ktab.toStream.to(WordCountAppConstants.outputStream, Produced.`with`(Serdes.String(), Serdes.Long()))

    streamBuilder
  }

  StreamsRunner.runWith(builder(), config())
}
