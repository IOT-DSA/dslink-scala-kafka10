package org.dsa.iot.kafka10

import scala.concurrent.Future
import scala.util.{ Failure, Success }

import org.apache.kafka.clients.producer.RecordMetadata
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.actions.{ ActionResult, EditorType, ResultType }
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.{ Value => DSAValue, ValueType }
import org.dsa.iot.scala._
import org.slf4j.LoggerFactory

/**
 * Application controller.
 */
class AppController(val connection: DSAConnection) {
  import Settings._
  import org.dsa.iot.dslink.node.value.ValueType._
  import java.lang.{ Integer => JInt, Long => JLong, Double => JDouble }

  type Binary = Array[Byte]

  private val log = LoggerFactory.getLogger(getClass)

  val root = connection.responderLink.getNodeManager.getSuperRoot

  initRoot

  log.info("Application controller started")

  /**
   * Initializes the root node.
   */
  private def initRoot() = {
    root createChild "addConnection" display "Add Connection" action addConnection build ()

    root.children.values filter isConnectionNode foreach initConnNode
  }

  /**
   * Initializes connection node.
   */
  private def initConnNode(node: Node) = {
    val name = node.getName
    val brokerUrl = node.configurations("brokerUrl").toString

    log.debug(s"Initializing connection node [$name, $brokerUrl]")

    val conn = KafkaConnection(name, brokerUrl)
    node.setMetaData(conn)

    node createChild "listTopics" display "List Topics" action listTopics build ()
    node createChild "getTopicInfo" display "Get Topic Info" action getTopicInfo build ()
    node createChild "publishString" display "Publish as Text" action publishString build ()
    node createChild "publishInt" display "Publish as Int" action publishInt build ()
    node createChild "publishBinary" display "Publish as Binary" action publishBinary build ()
    node createChild "removeConnection" display "Remove Connection" action removeConnection build ()

    log.info(s"Connection node [$name] initialized")
  }

  /**
   * Shuts down the controller and all active connections.
   */
  def shutdown() = {
    root.children.values filter isConnectionNode foreach { node =>
      val conn = node.getMetaData[KafkaConnection]
      conn.close
    }

    log.info("Application controller shut down")
  }

  /* actions */

  /**
   * Creates a new connection node.
   */
  lazy val addConnection = createAction(
    parameters = List(
      STRING("name") default "new_connection" description "Connection name",
      STRING("brokers") default DEFAULT_BROKER_URL description "Hosts with optional :port suffix"),
    handler = event => {
      val name = event.getParam[String]("name", !_.isEmpty, "Name cannot be empty").trim
      val brokerUrl = event.getParam[String]("brokers", !_.isEmpty, "Brokers cannot be empty").trim
      if (root.children.contains(name))
        throw new IllegalArgumentException(s"Duplicate connection name: $name")

      // throws exception if URL is invalid
      parseBrokerList(brokerUrl)

      val connNode = root createChild name config (NODE_TYPE -> CONNECTION, "brokerUrl" -> brokerUrl) build ()
      log.info(s"New connection node [$name, $brokerUrl] created")

      initConnNode(connNode)
    })

  /**
   * Removes connection from the tree.
   */
  lazy val removeConnection: ActionHandler = event => {
    val node = event.getNode.getParent
    val name = node.getName

    node.delete
    log.info(s"Connection node [$name] removed")
  }

  /**
   * Lists topics for the current connection.
   */
  lazy val listTopics = createAction(
    results = STRING("topicName") ~ NUMBER("partitions"),
    resultType = ResultType.TABLE,
    handler = kafkaAction { event =>
      val node = event.getNode.getParent

      val conn = node.getMetaData[KafkaConnection]
      val topics = conn.listTopics
      log.info(s"${topics.size} topics retrieved for connection [${conn.name}]")

      topics map {
        case (topicName, partitions) =>
          val values = List(topicName, partitions.size)
          Row.make(values.map(anyToValue): _*)
      }
    })

  /**
   * Returns a topic info.
   */
  lazy val getTopicInfo = createAction(
    parameters = STRING("topicName"),
    results = NUMBER("partition #") :: NUMBER("replication") :: STRING("replicas") :: STRING("leader") :: Nil,
    resultType = ResultType.TABLE,
    handler = kafkaAction { event =>
      val name = event.getParam[String]("topicName", !_.isEmpty, "Name cannot be empty").trim

      val node = event.getNode.getParent
      val conn = node.getMetaData[KafkaConnection]
      val partitions = conn.partitionsFor(name)
      log.info(s"${partitions.size} partitions retrieved for topic [$name]")

      partitions map { pi =>
        val replicas = pi.replicas map (_.toString) mkString ("[", ", ", "]")
        Row.make(pi.partition, pi.replicas.size, replicas, pi.leader.toString)
      }
    })

  /**
   * Auxiliary class to extract publishing information from the action result.
   */
  private case class PublishInfo(topicName: String, partition: Option[Int],
                                 timestamp: Option[Long], options: Map[String, String], flush: Boolean)
  private object PublishInfo {
    val params = List(
      STRING("topicName") description "Name of the topic to publish to",
      NUMBER("partition") description "Partition to send the message to (optional)",
      STRING("timestamp") description "Message timestamp (optional)",
      STRING("options") description "Additional options" default "",
      BOOL("flush") description "Flush after publishing" default true)

    def fromEvent(event: ActionResult) = {
      val topicName = event.getParam[String]("topicName", !_.isEmpty, "Topic name cannot be empty").trim
      val partition = getParamOption[Int](event, "partition")
      val timestamp = getParamOption[java.util.Date](event, "timestamp")(valueToDate) map (_.getTime)
      val flush = event.getParam[Boolean]("flush")
      val options = event.getParam[String]("options").split(",").map(_.trim).filterNot(_.isEmpty) map { str =>
        val parts = str.split("=").map(_.trim)
        parts(0) -> parts(1)
      } toMap

      apply(topicName, partition, timestamp, options, flush)
    }
  }

  /**
   * Publishes a string message.
   */
  lazy val publishString = createAction(
    parameters = List(
      STRING("key") description "Message key (optional)",
      STRING("value") description "Message value") ++ PublishInfo.params,
    results = STRING("status"),
    handler = kafkaAction { event =>
      val pi = PublishInfo.fromEvent(event)

      val key = getParamOption[String](event, "key")
      val value = event.getParam[String]("value")

      val node = event.getNode.getParent
      val conn = node.getMetaData[KafkaConnection]

      val frmd = conn.publish(pi.topicName, key, value, pi.partition, pi.timestamp, pi.options, pi.flush)
      List(Row.make(status(frmd)))
    })

  /**
   * Publishes an integer message.
   */
  lazy val publishInt = createAction(
    parameters = List(
      NUMBER("key") description "Message key (optional)",
      NUMBER("value") description "Message value") ++ PublishInfo.params,
    results = STRING("status"),
    handler = kafkaAction { event =>
      val pi = PublishInfo.fromEvent(event)

      val key = getParamOption[Int](event, "key") map (k => k: JInt)
      val value = event.getParam[Int]("value"): JInt

      val node = event.getNode.getParent
      val conn = node.getMetaData[KafkaConnection]

      val frmd = conn.publish(pi.topicName, key, value, pi.partition, pi.timestamp, pi.options, pi.flush)
      List(Row.make(status(frmd)))
    })

  /**
   * Publishes a byte array message.
   */
  lazy val publishBinary = createAction(
    parameters = List(
      BINARY("key") description "Message key (optional)",
      BINARY("value") description "Message value") ++ PublishInfo.params,
    results = STRING("status"),
    handler = kafkaAction { event =>
      val pi = PublishInfo.fromEvent(event)

      val key = getParamOption[Binary](event, "key")
      val value = event.getParam[Binary]("value")

      val node = event.getNode.getParent
      val conn = node.getMetaData[KafkaConnection]

      val frmd = conn.publish(pi.topicName, key, value, pi.partition, pi.timestamp, pi.options, pi.flush)
      List(Row.make(status(frmd)))
    })

  /* aux methods */

  /**
   * Extracts an optional parameter from an action result.
   */
  private def getParamOption[T](
    event: ActionResult, name: String)(implicit ex: DSAValue => T): Option[T] = event.getParameter(name) match {
    case null  => None
    case value => Some(ex(value))
  }

  /**
   * Returns a string status of the publish operation's result.
   */
  private def status(frmd: Future[RecordMetadata]) = frmd.value match {
    case None               => "Sent"
    case Some(Success(rmd)) => s"OK [partition=${rmd.partition}, offset=${rmd.offset}, ts=${rmd.timestamp}]"
    case Some(Failure(err)) => s"ERROR [${err.getMessage}]"
  }
}