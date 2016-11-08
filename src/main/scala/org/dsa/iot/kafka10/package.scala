package org.dsa.iot

import java.net.InetSocketAddress
import java.text.{ ParseException, SimpleDateFormat }

import _root_.scala.collection.JavaConverters.{ asScalaBufferConverter, seqAsJavaListConverter }
import _root_.scala.util.{ Failure, Success, Try }
import _root_.scala.util.control.NonFatal

import org.apache.kafka.clients.ClientUtils
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.serialization._
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.actions.ActionResult
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.{ Value => DSAValue }
import org.dsa.iot.kafka10.Settings
import org.dsa.iot.scala.{ RichNode, stringToValue }
import org.slf4j.LoggerFactory

/**
 * Types and utility functions for Kafka10.
 */
package object kafka10 {
  import Settings._

  type Binary = Array[Byte]

  private val log = LoggerFactory.getLogger(getClass)

  private val timeFormatters = List("yyyy-MM-dd'T'HH:mm:ssz", "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mmz", "yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd") map (p => new SimpleDateFormat(p))

  /* serializers */

  implicit val stringSerializer = new StringSerializer
  implicit val intSerializer = new IntegerSerializer
  implicit val binarySerializer = new ByteArraySerializer

  implicit val stringDeserializer = new StringDeserializer
  implicit val intDeserializer = new IntegerDeserializer
  implicit val binaryDeserializer = new ByteArrayDeserializer

  /* node helpers */

  val NODE_TYPE = "nodeType"
  val CONNECTION = "connection"
  val TOPIC = "topic"
  val BASIC_SUB = "basicSubcription"
  val ADVANCED_SUB = "advancedSubscription"

  /**
   * Returns the type of the node.
   */
  def getNodeType(node: Node) = node.configurations.get(NODE_TYPE) map (_.asInstanceOf[String])

  /**
   * Checks if the node type is `connection`.
   */
  def isConnectionNode(node: Node) = getNodeType(node) == Some(CONNECTION)

  /**
   * Checks if the node type is `topic`.
   */
  def isTopicNode(node: Node) = getNodeType(node) == Some(TOPIC)

  /**
   * Checks if the node type is `basicSubscription`.
   */
  def isBasicSubNode(node: Node) = getNodeType(node) == Some(BASIC_SUB)

  /**
   * Checks if the node type is `advancedSubscription`.
   */
  def isAdvancedSubNode(node: Node) = getNodeType(node) == Some(ADVANCED_SUB)

  /* utility methods */

  /**
   * Retries executing some code up to the specified number of times.
   *
   * @param n how many times to try before throwing an exception.
   * @param timeout time to wait before retrying (if `exponential` is set, this will be the initial timeout).
   * @param exponential if `true`, then each subsequent timeout will be twice as long as the previous one.
   */
  @annotation.tailrec
  def retry[T](n: Int, timeout: Long = 10, exponential: Boolean = true)(fn: => T): T = Try { fn } match {
    case Success(x) => x
    case _ if n > 1 =>
      log.warn(s"Operation failed, waiting for $timeout ms to retry")
      if (timeout > 0) Thread.sleep(timeout)
      val newTimeout = if (exponential) timeout * 2 else timeout
      retry(n - 1, newTimeout, exponential)(fn)
    case Failure(e) => throw e
  }

  /**
   * Parses a comma separated list of broker addresses, each address be either `host:port` or simply `host`,
   * in which case the default port [[Settings.DEFAULT_PORT]] is used.
   */
  def parseBrokerList(brokerUrl: String): List[InetSocketAddress] = {
    val urls = brokerUrl split ("\\s*,\\s*") map (_.trim) filterNot (_.isEmpty) map {
      case url if url.contains(":") => url
      case url                      => url + ":" + DEFAULT_PORT
    }

    ClientUtils.parseAndValidateAddresses(urls.toList.asJava).asScala.toList
  }

  /**
   * Executes the body of the action and presents the result in the response table.
   * If the action fails, it retries it several times ([[Settings.ACTION_RETRIES]]).
   */
  def kafkaAction(body: ActionResult => Iterable[Row]) = (event: ActionResult) => try {
    val rows = body(event)
    rows foreach event.getTable.addRow
  } catch {
    case e: RetriableException =>
      val rows = retry(Settings.ACTION_RETRIES)(body(event))
      rows foreach event.getTable.addRow
    case e: KafkaException => event.getTable.addRow(Row.make("Kafka error: " + e.getMessage))
    case NonFatal(e)       => event.getTable.addRow(Row.make(e.getMessage))
  }

  /**
   * Returns Some(str) if the argument is a non-empty string, None otherwise.
   */
  def noneIfEmpty(str: String) = Option(str) filter (!_.trim.isEmpty)

  /**
   * Splits the argument into chunks with the specified delimiter, trims each part and returns only non-empty parts.
   */
  def splitAndTrim(delim: String = ",")(str: String) = str.split(delim).map(_.trim).filterNot(_.isEmpty).toList

  /**
   * Tries to parse a string into a time using one of the available formats.
   */
  @throws(classOf[ParseException])
  def parseTime(str: String) = timeFormatters.foldLeft[Try[java.util.Date]](Failure(null)) { (parsed, fmt) =>
    parsed match {
      case yes @ Success(_) => yes
      case no @ Failure(_)  => Try(fmt.parse(str))
    }
  } get

  /**
   * Parses a value into a java Date instance.
   */
  def valueToDate(v: DSAValue) = parseTime(v.getString)

  /**
   * Parses a value into an enumeration item.
   */
  def valueToEnum(e: Enumeration)(v: DSAValue) = e.withName(v.getString)
}