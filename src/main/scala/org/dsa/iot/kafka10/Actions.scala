package org.dsa.iot.kafka10

import java.lang.{ Integer => JInt, Long => JLong, Double => JDouble }

import scala.concurrent.Future
import scala.util.{ Failure, Success }

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.PartitionInfo
import org.dsa.iot.dslink.node.actions.{ ActionResult, Parameter, ResultType }
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.{ Value => DSAValue, ValueType }
import org.dsa.iot.dslink.node.value.ValueType.{ BINARY, BOOL, NUMBER, STRING }
import org.dsa.iot.scala._
import org.slf4j.LoggerFactory

import Settings.DEFAULT_BROKER_URL

/**
 * Actions common class.
 */
abstract class ActionsBase(ctrl: AppController) {

  protected val log = LoggerFactory.getLogger(getClass)

  /**
   * Creates an action to retrive topic info.
   */
  protected def createGetTopicInfoAction(
    actionParams: List[Parameter], accessor: ActionResult => (String, List[PartitionInfo])) = createAction(
    parameters = actionParams,
    results = NUMBER("partition #") :: NUMBER("replication") :: STRING("replicas") :: STRING("leader") :: Nil,
    resultType = ResultType.TABLE,
    handler = kafkaAction { event =>
      val (name, partitions) = accessor(event)
      log.info(s"${partitions.size} partitions retrieved for topic [$name]")

      partitions map { pi =>
        val replicas = pi.replicas map (_.toString) mkString ("[", ", ", "]")
        Row.make(pi.partition, pi.replicas.size, replicas, pi.leader.toString)
      }
    })

  /**
   * Creates an action for publishing message onto a topic.
   */
  protected def createPublishAction(
    keyType: ValueType, valueType: ValueType, withTopicParam: Boolean,
    publisher: (Option[Int], Option[Long], Boolean, Map[String, String], ActionResult) => Future[RecordMetadata]) = {

    val keyValueParams = List(
      keyType("key") description "Message key (optional)",
      valueType("value") description "Message value")
    val topicParam = if (withTopicParam) List(STRING("topicName") description "Name of the topic to publish to") else Nil
    val otherParams = List(
      NUMBER("partition") description "Partition to send the message to (optional)",
      STRING("timestamp") description "Message timestamp (optional)",
      STRING("options") description "Additional options" default "",
      BOOL("flush") description "Flush after publishing" default true)

    createAction(
      parameters = keyValueParams ++ topicParam ++ otherParams,
      results = STRING("status"),
      handler = kafkaAction { event =>
        val partition = getParamOption[Int](event, "partition")
        val timestamp = getParamOption[java.util.Date](event, "timestamp")(valueToDate) map (_.getTime)
        val flush = event.getParam[Boolean]("flush")
        val options = parseParamAsList(event, "options")

        val frmd = publisher(partition, timestamp, flush, options, event)
        List(Row.make(status(frmd)))
      })
  }

  /**
   * Extracts an optional parameter from the action result.
   */
  protected def getParamOption[T](
    event: ActionResult, name: String)(implicit ex: DSAValue => T): Option[T] = event.getParameter(name) match {
    case null  => None
    case value => Some(ex(value))
  }

  /**
   * Extracts a mandatory string parameter from the action result.
   */
  protected def getRequiredStringParam(event: ActionResult, name: String) = event.getParam[String](name, !_.isEmpty, s"$name cannot be empty").trim

  /**
   * Parses a string parameter into a list of strings, using the supplied separator
   */
  protected def parseParamAsList(event: ActionResult, name: String, entrySep: String = ",", keyValueSet: String = "=") =
    event.getParam[String](name).split(entrySep).map(_.trim).filterNot(_.isEmpty) map { str =>
      val parts = str.split(keyValueSet).map(_.trim)
      parts(0) -> parts(1)
    } toMap

  /**
   * Retrieves the action's parent node.
   */
  protected def getParent(event: ActionResult) = event.getNode.getParent

  /**
   * Retrieves the metadata from the action's parent node.
   */
  protected def getParentMeta[T](event: ActionResult) = getParent(event).getMetaData[T]

  /**
   * Returns a string status of the publish operation's result.
   */
  protected def status(frmd: Future[RecordMetadata]) = frmd.value match {
    case None               => "Sent"
    case Some(Success(rmd)) => s"OK [partition=${rmd.partition}, offset=${rmd.offset}, ts=${rmd.timestamp}]"
    case Some(Failure(err)) => s"ERROR [${err.getMessage}]"
  }
}

/**
 * Actions for Connection nodes.
 */
class ConnectionActions(ctrl: AppController) extends ActionsBase(ctrl) {

  /**
   * Creates a new connection node.
   */
  lazy val ADD_CONNECTION = createAction(
    parameters = List(
      STRING("connName") default "new_connection" description "Connection name",
      STRING("brokers") default DEFAULT_BROKER_URL description "Hosts with optional :port suffix"),
    handler = event => {
      val name = getRequiredStringParam(event, "connName")
      val brokerUrl = getRequiredStringParam(event, "brokers")

      val parent = getParent(event)
      if (parent.children.contains(name))
        throw new IllegalArgumentException(s"Duplicate connection name: $name")

      ctrl.addConnNode(parent)(name, brokerUrl)
    })

  /**
   * Removes the connection node.
   */
  lazy val REMOVE_CONNECTION: ActionHandler = getParent _ andThen ctrl.removeConnNode

  /**
   * Lists topics for the current connection.
   */
  lazy val LIST_TOPICS = createAction(
    parameters = BOOL("createNodes") description "Create nodes for each discovered topic" default false,
    results = STRING("topicName") :: NUMBER("partitions") :: Nil,
    resultType = ResultType.TABLE,
    handler = kafkaAction { event =>
      val createNodes = event.getParam[Boolean]("createNodes")

      val conn = getParentMeta[KafkaConnection](event)
      val topics = conn.listTopics
      log.info(s"${topics.size} topics retrieved for connection [${conn.name}]")

      if (createNodes)
        topics.keys foreach ctrl.addTopicNode(getParent(event))

      topics map {
        case (topicName, partitions) =>
          val values = List(topicName, partitions.size)
          Row.make(values.map(anyToValue): _*)
      }
    })

  /**
   * Retrieves topic information.
   */
  lazy val GET_TOPIC_INFO = createGetTopicInfoAction(
    List(STRING("topicName") description "Name of the topic to query"),
    event => {
      val name = getRequiredStringParam(event, "topicName")
      val conn = getParentMeta[KafkaConnection](event)
      (name, conn.partitionsFor(name))
    })

  /**
   * Creates a new topic node.
   */
  lazy val ADD_TOPIC = createAction(
    parameters = STRING("topicName") description "Name of the topic to add",
    handler = event => {
      val name = event.getParam[String]("topicName", !_.isEmpty, "Name cannot be empty").trim
      ctrl.addTopicNode(getParent(event))(name)
    })

  /**
   * Publishes a string message.
   */
  lazy val PUBLISH_STRING = createPublishAction(STRING, STRING, true,
    (partition, timestamp, flush, options, event) => {
      val key = getParamOption[String](event, "key")
      val value = event.getParam[String]("value")
      val topicName = getRequiredStringParam(event, "topicName")

      val conn = getParentMeta[KafkaConnection](event)
      conn.publish(topicName, key, value, partition, timestamp, options, flush)
    })

  /**
   * Publishes an integer message.
   */
  lazy val PUBLISH_INT = createPublishAction(NUMBER, NUMBER, true,
    (partition, timestamp, flush, options, event) => {
      val key = getParamOption[Int](event, "key") map (k => k: JInt)
      val value = event.getParam[Int]("value"): JInt
      val topicName = getRequiredStringParam(event, "topicName")

      val conn = getParentMeta[KafkaConnection](event)
      conn.publish(topicName, key, value, partition, timestamp, options, flush)
    })

  /**
   * Publishes a byte array message.
   */
  lazy val PUBLISH_BINARY = createPublishAction(BINARY, BINARY, true,
    (partition, timestamp, flush, options, event) => {
      val key = getParamOption[Binary](event, "key")
      val value = event.getParam[Binary]("value")
      val topicName = getRequiredStringParam(event, "topicName")

      val conn = getParentMeta[KafkaConnection](event)
      conn.publish(topicName, key, value, partition, timestamp, options, flush)
    })
}

/**
 * Actions for Topic nodes.
 */
class TopicActions(ctrl: AppController) extends ActionsBase(ctrl) {

  /**
   * Retrieves topic information.
   */
  lazy val GET_TOPIC_INFO = createGetTopicInfoAction(
    Nil,
    event => {
      val topic = getParentMeta[KafkaTopic](event)
      (topic.name, topic.partitions)
    })

  /**
   * Removes the topic node.
   */
  lazy val REMOVE_TOPIC: ActionHandler = getParent _ andThen ctrl.removeTopicNode

  /**
   * Publishes a string message.
   */
  lazy val PUBLISH_STRING = createPublishAction(STRING, STRING, false,
    (partition, timestamp, flush, options, event) => {
      val key = getParamOption[String](event, "key")
      val value = event.getParam[String]("value")

      val topic = getParentMeta[KafkaTopic](event)
      topic.publish(key, value, partition, timestamp, options, flush)
    })

  /**
   * Publishes an integer message.
   */
  lazy val PUBLISH_INT = createPublishAction(NUMBER, NUMBER, false,
    (partition, timestamp, flush, options, event) => {
      val key = getParamOption[Int](event, "key") map (k => k: JInt)
      val value = event.getParam[Int]("value"): JInt

      val topic = getParentMeta[KafkaTopic](event)
      topic.publish(key, value, partition, timestamp, options, flush)
    })

  /**
   * Publishes a byte array message.
   */
  lazy val PUBLISH_BINARY = createPublishAction(BINARY, BINARY, false,
    (partition, timestamp, flush, options, event) => {
      val key = getParamOption[Binary](event, "key")
      val value = event.getParam[Binary]("value")

      val topic = getParentMeta[KafkaTopic](event)
      topic.publish(key, value, partition, timestamp, options, flush)
    })
}