package org.dsa.iot.kafka10

import java.lang.{ Integer => JInt, Long => JLong, Double => JDouble }
import scala.concurrent.Future
import scala.util.{ Failure, Success }
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common._
import org.dsa.iot.dslink.node.actions.{ ActionResult, Parameter, ResultType }
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.{ Value => DSAValue, ValueType }
import org.dsa.iot.dslink.node.value.ValueType.{ BINARY, BOOL, NUMBER, STRING, MAP }
import org.dsa.iot.scala._
import org.slf4j.LoggerFactory
import Settings.{ CONSUMER_CONFIG, DEFAULT_BROKER_URL }
import org.apache.kafka.clients.consumer.ConsumerConfig

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
        val options = parseParamAsMap(event, "options")

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
   * Parses a string parameter into a list of strings using the supplied separator.
   */
  protected def parseParamAsList(event: ActionResult, name: String, sep: String = ",") =
    event.getParam[String](name).split(sep).map(_.trim).filterNot(_.isEmpty).toList

  /**
   * Parses a string parameter into a map of strings, using the supplied separator for entries and key/values.
   */
  protected def parseParamAsMap(event: ActionResult, name: String, entrySep: String = ",", keyValueSet: String = "=") =
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
      val name = getRequiredStringParam(event, "topicName")

      val parent = getParent(event)
      if (parent.children.contains(name))
        throw new IllegalArgumentException(s"Duplicate topic name: $name")

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

  /**
   * Subscribes to a list of topics/partitions.
   */
  lazy val SUBSCRIBE_BINARY = createAction(
    parameters = List(
      STRING("name") description "Subscription name, can't be empty",
      MAP("partitions") description "Map \"topic\":[list of partitions]",
      STRING("groupId") description "Consumer Group Id" default CONSUMER_CONFIG.getString("group.id"),
      BOOL("autoCommit") description "Automatically commit offsets" default true,
      STRING("options") description "Additional options" default ""),
    handler = event => {
      val name = getRequiredStringParam(event, "name")
      val groupId = getRequiredStringParam(event, "groupId")
      val autoCommit = event.getParam[Boolean]("autoCommit")
      val options = parseParamAsMap(event, "options") +
        (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> autoCommit.toString)
      val partitions = event.getParam[Map[String, _]]("partitions") map {
        case (topic, list: Iterable[_]) => topic -> list.asInstanceOf[Iterable[Int]]
      }

      val parent = getParent(event)

      ctrl.addAdvancedSubNode(parent)(name, groupId, partitions, options)
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

  /**
   * Subscribes to the topic for receiving messages as strings.
   */
  lazy val SUBSCRIBE_STRING = createSubscribeAction(STRING)

  /**
   * Subscribes to the topic for receiving messages as integers.
   */
  lazy val SUBSCRIBE_INT = createSubscribeAction(NUMBER)

  /**
   * Subscribes to the topic for receiving messages as byte arrays.
   */
  lazy val SUBSCRIBE_BINARY = createSubscribeAction(BINARY)

  /**
   * Creates an action for subscribing to a topic.
   */
  protected def createSubscribeAction(dataType: ValueType) = createAction(
    parameters = List(
      STRING("groupId") description "Consumer Group Id" default CONSUMER_CONFIG.getString("group.id"),
      STRING("options") description "Additional options" default ""),
    handler = event => {
      val groupId = getRequiredStringParam(event, "groupId")
      val options = parseParamAsMap(event, "options")

      val parent = getParent(event)

      ctrl.addBasicSubNode(parent)(groupId, dataType, options)
    })
}

/**
 * Actions for Basic Subscription nodes.
 */
class BasicSubscriptionActions(ctrl: AppController) extends ActionsBase(ctrl) {

  /**
   * Starts streaming from Kafka.
   */
  lazy val START: ActionHandler = event => getParentMeta[BasicSubscription[_, _]](event).start

  /**
   * Stops streaming from Kafka.
   */
  lazy val STOP: ActionHandler = event => getParentMeta[BasicSubscription[_, _]](event).stop

  /**
   * Removes the subscription node.
   */
  lazy val REMOVE_SUBSCRIPTION: ActionHandler = getParent _ andThen ctrl.removeBasicSubNode
}

/**
 * Actions for Advanced Subscription nodes.
 */
class AdvancedSubscriptionActions(ctrl: AppController) extends ActionsBase(ctrl) {
  /**
   * Starts streaming from Kafka.
   */
  lazy val START: ActionHandler = event => getParentMeta[AdvancedSubscription[_, _]](event).start

  /**
   * Stops streaming from Kafka.
   */
  lazy val STOP: ActionHandler = event => getParentMeta[AdvancedSubscription[_, _]](event).stop

  /**
   * Sets the consumer position to a certain offset.
   */
  lazy val SEEK = createAction(
    parameters = List(
      BOOL("allPartitions") description "Reset position for all partitions" default true,
      STRING("topic") description "Topic to change position for",
      NUMBER("partition") description "Partition to change position for" default 0,
      ENUMS(OffsetType)("offsetType") default OffsetType.Latest,
      NUMBER("offset") description "Custom offset" default 0),
    handler = event => {
      val all = event.getParam[Boolean]("allPartitions")
      val topic = getParamOption[String](event, "topic")
      val partition = getParamOption[Int](event, "partition")
      val offsetType = OffsetType withName event.getParam[String]("offsetType")
      val customOffset = event.getParam[Number]("offset").longValue

      val sub = getParentMeta[AdvancedSubscription[_, _]](event)

      val partitions = if (all) Nil else List(new TopicPartition(topic.orNull, partition.getOrElse(0)))
      sub.seek(partitions, offsetType, customOffset)
    })

  /**
   * Commits consumer offsets.
   */
  lazy val COMMIT_OFFSETS: ActionHandler = event => getParentMeta[AdvancedSubscription[_, _]](event).commit(true)

  /**
   * Removes the subscription node.
   */
  lazy val REMOVE_SUBSCRIPTION: ActionHandler = getParent _ andThen ctrl.removeAdvancedSubNode
}