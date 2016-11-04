package org.dsa.iot.kafka10

import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.value.ValueType
import org.dsa.iot.dslink.node.value.ValueType.{ BINARY, NUMBER, STRING }
import org.dsa.iot.scala._
import org.slf4j.LoggerFactory

/**
 * Application controller.
 */
class AppController(val connection: DSAConnection) {

  protected val log = LoggerFactory.getLogger(getClass)

  protected val connActions = new ConnectionActions(this)
  protected val topicActions = new TopicActions(this)
  protected val subscriptionActions = new SubscriptionActions(this)

  val root = connection.responderLink.getNodeManager.getSuperRoot

  initRoot

  log.info("Application controller started")

  /**
   * Initializes the root node.
   */
  private def initRoot() = {
    root createChild "addConnection" display "Add Connection" action connActions.ADD_CONNECTION build ()

    root.children.values filter isConnectionNode foreach initConnNode
  }

  /**
   * Adds a connection node to the parent.
   */
  def addConnNode(parent: Node)(name: String, brokerUrl: String) = {
    parseBrokerList(brokerUrl) // throws exception if URL is invalid

    val connNode = parent createChild name config (NODE_TYPE -> CONNECTION, "brokerUrl" -> brokerUrl) build ()
    log.info(s"New connection node [$name, $brokerUrl] created")

    initConnNode(connNode)
  }

  /**
   * Initializes connection node.
   */
  def initConnNode(node: Node) = {
    import connActions._

    val name = node.getName
    val brokerUrl = node.configurations("brokerUrl").toString

    log.debug(s"Initializing connection node [$name, $brokerUrl]")

    val conn = KafkaConnection(name, brokerUrl)
    node.setMetaData(conn)

    node createChild "listTopics" display "List Topics" action LIST_TOPICS build ()
    node createChild "getTopicInfo" display "Get Topic Info" action GET_TOPIC_INFO build ()
    node createChild "addTopic" display "Add Topic" action ADD_TOPIC build ()
    node createChild "publishString" display "Publish as Text" action PUBLISH_STRING build ()
    node createChild "publishInt" display "Publish as Int" action PUBLISH_INT build ()
    node createChild "publishBinary" display "Publish as Binary" action PUBLISH_BINARY build ()
    node createChild "removeConnection" display "Remove" action REMOVE_CONNECTION build ()

    node.children.values filter isTopicNode foreach initTopicNode

    log.info(s"Connection node [$name] initialized")
  }

  /**
   * Closes the connection node.
   */
  def closeConnNode(node: Node) = {
    node.children.values filter isTopicNode foreach closeTopicNode
    node.getMetaData[KafkaConnection].close
    log.info(s"Connection node [${node.getName}] closed")
  }

  /**
   * Removes the connection node from the tree.
   */
  def removeConnNode(node: Node) = {
    val name = node.getName

    node.children.values filter isTopicNode foreach removeTopicNode
    closeConnNode(node)

    node.delete
    log.info(s"Connection node [$name] removed")
  }

  /**
   * Adds a topic node to the parent.
   */
  def addTopicNode(parent: Node)(name: String) = {
    val topicNode = parent createChild name config (NODE_TYPE -> TOPIC) build ()
    log.info(s"New topic node [$name] created")

    initTopicNode(topicNode)
  }

  /**
   * Initializes topic node.
   */
  def initTopicNode(node: Node) = {
    import topicActions._

    val name = node.getName

    log.debug(s"Initializing topic node [$name]")

    val conn = node.getParent.getMetaData[KafkaConnection]
    val topic = KafkaTopic(name, conn)
    node.setMetaData(topic)

    node createChild "getTopicInfo" display "Get Info" action GET_TOPIC_INFO build ()
    node createChild "publishString" display "Publish as Text" action PUBLISH_STRING build ()
    node createChild "publishInt" display "Publish as Int" action PUBLISH_INT build ()
    node createChild "publishBinary" display "Publish as Binary" action PUBLISH_BINARY build ()
    node createChild "subsribeString" display "Subscribe as String" action SUBSCRIBE_STRING build ()
    node createChild "subsribeInt" display "Subscribe as Int" action SUBSCRIBE_INT build ()
    node createChild "subsribeBinary" display "Subscribe as Binary" action SUBSCRIBE_BINARY build ()
    node createChild "removeTopic" display "Remove" action REMOVE_TOPIC build ()

    node.children.values filter isBasicSubNode foreach initSubscriptionNode

    log.info(s"Topic node [$name] initialized")
  }

  /**
   * Closes the topic node.
   */
  def closeTopicNode(node: Node) = {
    node.children.values filter isBasicSubNode foreach closeSubscriptionNode
    node.getMetaData[KafkaTopic].close
    log.info(s"Topic node [${node.getName}] closed")
  }

  /**
   * Removes the topic node from the tree.
   */
  def removeTopicNode(node: Node) = {
    val name = node.getName

    node.children.values filter isBasicSubNode foreach removeSubscriptionNode
    closeTopicNode(node)

    node.delete
    log.info(s"Topic [$name] removed")
  }

  /**
   * Adds a subscription node to the parent.
   */
  def addSubscriptionNode(parent: Node)(groupId: String, dataType: ValueType, options: Map[String, String]) = {
    val subIndices = parent.children.values filter isBasicSubNode collect {
      case node if node.configurations("groupId") == groupId => node.configurations("index").asInstanceOf[Int]
    }
    val index = if (subIndices.isEmpty) 0 else subIndices.max + 1
    val name = groupId + " #" + index

    val subNode = parent createChild name config (NODE_TYPE -> BASIC_SUB, "groupId" -> groupId,
      "index" -> index, "dataType" -> dataType.getRawName, "options" -> options) build ()
    log.info(s"New subscription node [$name] created")

    subNode createChild "key" valueType dataType build ()
    subNode createChild "value" valueType dataType build ()

    initSubscriptionNode(subNode)
  }

  /**
   * Initializes subscription node.
   */
  def initSubscriptionNode(node: Node) = {
    import subscriptionActions._

    val name = node.getName
    val groupId = node.configurations("groupId").toString
    val options = node.configurations("options").asInstanceOf[Map[String, String]]
    val dataType = ValueType.toValueType(node.configurations("dataType").toString)

    log.debug(s"Initializing subscription node [$name]")

    val topic = node.getParent.getMetaData[KafkaTopic]
    val sub = dataType match {
      case STRING => new BasicSubscription[String, String](name, groupId, options, topic)
      case NUMBER => new BasicSubscription[Integer, Integer](name, groupId, options, topic)
      case BINARY => new BasicSubscription[Binary, Binary](name, groupId, options, topic)
    }

    sub.output.subscribe { evt =>
      node.children("key") setValue anyToValue(evt.key)
      node.children("value") setValue anyToValue(evt.value)
      node.setAttribute("timestamp", new java.util.Date(evt.timestamp).toString)
      node.setAttribute("checksum", evt.checksum)
      node.setAttribute(s"offset #${evt.partition}", evt.offset)
    }
    node.setMetaData(sub)

    node createChild "startStreaming" display "Start" action START build ()
    node createChild "stopStreaming" display "Stop" action STOP build ()
    node createChild "removeSubscription" display "Remove" action REMOVE_SUBSCRIPTION build ()

    log.info(s"Subscription node [$name] initialized")
  }

  /**
   * Closes the subscription node.
   */
  def closeSubscriptionNode(node: Node) = {
    node.getMetaData[BasicSubscription[_, _]].close
    log.info(s"Subscription node [${node.getName}] closed")
  }

  /**
   * Removes the subscription node from the tree.
   */
  def removeSubscriptionNode(node: Node) = {
    val name = node.getName

    closeSubscriptionNode(node)

    node.delete
    log.info(s"Subscription node [$name] removed")
  }

  /**
   * Shuts down the controller and all active connections.
   */
  def shutdown() = {
    root.children.values filter isConnectionNode foreach closeConnNode
    log.info("Application controller shut down")
  }
}