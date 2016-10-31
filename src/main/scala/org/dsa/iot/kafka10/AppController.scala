package org.dsa.iot.kafka10

import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.value.ValueType
import org.dsa.iot.scala.{ DSAConnection, RichNode, RichNodeBuilder }
import org.slf4j.LoggerFactory

/**
 * Application controller.
 */
class AppController(val connection: DSAConnection) {

  protected val log = LoggerFactory.getLogger(getClass)

  protected val connActions = new ConnectionActions(this)
  protected val topicActions = new TopicActions(this)

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
   * Removes the connection node from the tree.
   */
  def removeConnNode(node: Node) = {
    val name = node.getName

    val conn = node.getMetaData[KafkaConnection]
    conn.close

    node.delete
    log.info(s"Connection node [$name] removed")
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
    node createChild "removeConnection" display "Remove Connection" action REMOVE_CONNECTION build ()

    node.children.values filter isTopicNode foreach initTopicNode

    log.info(s"Connection node [$name] initialized")
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
  private def initTopicNode(node: Node) = {
    import topicActions._

    val name = node.getName

    log.debug(s"Initializing topic node [$name]")

    val conn = node.getParent.getMetaData[KafkaConnection]
    val topic = KafkaTopic(name, conn)
    node.setMetaData(topic)

    node createChild "getTopicInfo" display "Get Topic Info" action GET_TOPIC_INFO build ()
    node createChild "publishString" display "Publish as Text" action PUBLISH_STRING build ()
    node createChild "publishInt" display "Publish as Int" action PUBLISH_INT build ()
    node createChild "publishBinary" display "Publish as Binary" action PUBLISH_BINARY build ()
    node createChild "removeTopic" display "Remove Topic" action REMOVE_TOPIC build ()

    log.info(s"Topic node [$name] initialized")
  }

  /**
   * Removes the topic node from the tree.
   */
  def removeTopicNode(node: Node) = {
    val name = node.getName

    node.delete
    log.info(s"Topic [$name] removed")
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
}