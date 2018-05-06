package kafka.streams.enricher

import kafka.streams.common.ProducerBuilder
import org.apache.kafka.clients.producer.ProducerRecord

object UserDataProducer extends App {
  private def userRecord(user: String, userInfo: String) = new ProducerRecord[String, String](EnricherAppConstants.userTable, user, userInfo)

  private def purchaseRecord(user: String, items: String) = new ProducerRecord[String, String](EnricherAppConstants.purchaseStream, user, items)

  val producer = ProducerBuilder.buildFor(EnricherAppConstants.producerName)

  //TODO: send more data to simulate

  producer.send(userRecord("john", "someInfo")).get()
  producer.send(purchaseRecord("john", "some items")).get()
  Thread.sleep(100)

  producer.send(userRecord("alice", "some other info")).get()
  // this deletes user (null value for a table)
  producer.send(userRecord("alice", null)).get()
  producer.send(purchaseRecord("alice", "some other items")).get()

  sys.addShutdownHook(producer.close())
}
