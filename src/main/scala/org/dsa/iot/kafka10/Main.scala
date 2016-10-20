package org.dsa.iot.kafka10

import org.dsa.iot.dslink.DSLinkHandler
import org.dsa.iot.scala.{ DSAConnector, LinkMode }
import org.slf4j.LoggerFactory

/**
 * Kafka 0.10 DSLink.
 */
object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    log.info("Command line: " + args.mkString(" "))

    val connector = DSAConnector(args)
    val connection = connector start LinkMode.RESPONDER

    val controller = new AppController(connection)

    waitForShutdown(controller, connector)
  }

  private def waitForShutdown(controller: AppController, connector: DSAConnector) = {
    println("\nPress ENTER to exit")
    Console.in.readLine
    controller.shutdown
    connector.stop
    sys.exit(0)
  }
}

/**
 * A dummy class used only to ensure the validation of dslink.json
 */
class DummyDSLinkHandler extends DSLinkHandler