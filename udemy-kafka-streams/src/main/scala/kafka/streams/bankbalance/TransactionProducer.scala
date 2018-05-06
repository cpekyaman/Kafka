package kafka.streams.bankbalance

import kafka.streams.common.ProducerBuilder
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable
import scala.util.Random

object TransactionProducer extends App {
  private val customers = List("john", "william", "sarah", "alex", "tom", "alice")

  private val rand = new Random()

  private def randomAmount: Int = 10 + rand.nextInt(50)

  private def randomCustomer: String = customers(rand.nextInt(customers.size))

  private def message(customer: String): String = {
    val amount = randomAmount
    val now = java.time.format.DateTimeFormatter.ISO_DATE_TIME.format(java.time.Instant.now)

    //TODO: formatting not working with \ escaped " inside
    f"{name : $customer%s, amount : $amount%d, time : $now}"
  }

  val producer = ProducerBuilder.buildFor(BankBalanceAppConstants.producerName)

  var go = true
  for (customer <- immutable.Stream.continually(randomCustomer) if go) {
    try {
      producer.send(new ProducerRecord[String, String](BankBalanceAppConstants.inputTopic, customer, message(customer)))
      Thread.sleep(100 + rand.nextInt(100))
    } catch {
      case _: InterruptedException => go = false
    }
  }

  sys.addShutdownHook(producer.close())
}
