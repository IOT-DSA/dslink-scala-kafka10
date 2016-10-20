package org.dsa.iot.kafka10

import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.actions.ResultType
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.ValueType
import org.dsa.iot.scala._
import org.slf4j.LoggerFactory

/**
 * Application controller.
 */
class AppController(val connection: DSAConnection) {
  import Settings._
  import org.dsa.iot.dslink.node.value.ValueType._

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

  lazy val removeConnection: ActionHandler = event => {
    val node = event.getNode.getParent
    val name = node.getName

    node.delete
    log.info(s"Connection node [$name] removed")
  }

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

  lazy val getTopicInfo = createAction(
    parameters = STRING("topicName"),
    results = NUMBER("partition #") :: NUMBER("replication") :: STRING("replicas") :: STRING("leader") :: Nil,
    resultType = ResultType.TABLE,
    handler = kafkaAction { event =>
      val name = event.getParam[String]("topicName", !_.isEmpty, "Name cannot be empty").trim

      val node = event.getNode.getParent
      val conn = node.getMetaData[KafkaConnection]
      val partitions = conn.partitionsFor(name)
      partitions map { pi =>
        val replicas = pi.replicas map (_.toString) mkString ("[", ", ", "]")
        Row.make(pi.partition, pi.replicas.size, replicas, pi.leader.toString)
      }
    })
}