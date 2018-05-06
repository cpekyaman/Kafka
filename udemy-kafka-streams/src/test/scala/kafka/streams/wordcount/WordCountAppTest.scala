package kafka.streams.wordcount

import kafka.streams.common.StreamsRunner
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.junit.{After, Before, Test}

//TODO: fails with java.nio.file.DirectoryNotEmptyException: \tmp\kafka-streams\kafka-streams-wordcount\0_0 error
class WordCountAppTest {
  private var testDriver: TopologyTestDriver = _
  private var consumerRecordFactory: ConsumerRecordFactory[String, String] = _

  @Before
  def setup(): Unit = {
    val topology = WordCountApp.builder().build()
    val properties = StreamsRunner.propsFor(WordCountAppConstants.appName)

    val serde = new StringSerializer
    consumerRecordFactory = new ConsumerRecordFactory[String, String](serde, serde)
    testDriver = new TopologyTestDriver(topology, properties)
  }

  @After
  def tearDown(): Unit = {
    testDriver.close()
  }

  @Test
  def testCountsAreCorrect(): Unit = {
    val value = "testing Kafka streams"
    publishInput(value)
    OutputVerifier.compareKeyValue(readOutput(), "testing", new java.lang.Long(1))
    OutputVerifier.compareKeyValue(readOutput(), "kafka", new java.lang.Long(1))
  }

  def publishInput(value: String): Unit = {
    testDriver.pipeInput(consumerRecordFactory.create(WordCountAppConstants.inputStream, null, value))
  }

  def readOutput(): ProducerRecord[String, java.lang.Long] = {
    testDriver.readOutput(WordCountAppConstants.outputStream, new StringDeserializer, new LongDeserializer)
  }
}
