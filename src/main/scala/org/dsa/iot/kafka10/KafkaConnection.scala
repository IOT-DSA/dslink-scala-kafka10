package org.dsa.iot.kafka10

import scala.collection.JavaConverters._
import scala.concurrent.Future

import org.apache.kafka.clients.producer.{ ProducerConfig, RecordMetadata }
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.{ Serializer, StringDeserializer }
import org.dsa.iot.scala.Having
import org.slf4j.LoggerFactory

import com.typesafe.config.ConfigFactory

import cakesolutions.kafka.{ KafkaConsumer, KafkaProducer, KafkaProducerRecord }
import cakesolutions.kafka.KafkaProducerRecord.Destination

/**
 * Encapsulates Kafka connection info and operations.
 */
case class KafkaConnection(name: String, brokerUrl: String) {
  import Settings._
  import org.apache.kafka.clients.producer.ProducerConfig._

  type TopicsWithPartitions = Map[String, List[PartitionInfo]]

  private val log = LoggerFactory.getLogger(getClass)

  private lazy val auxConsumer = {
    val keyDSer = new StringDeserializer
    val valDSer = new StringDeserializer
    val conf = KafkaConsumer.Conf(keyDSer, valDSer, brokerUrl, AUX_GROUP_ID)
    KafkaConsumer(conf)
  }

  /**
   * Cleans up the connection artifacts.
   */
  def close(): Unit = {
    auxConsumer.close
    log.info("Aux consumer closed")
  }

  /**
   * List all topics with their partitions.
   *
   * @return the topics with associated lists of partition information records.
   */
  def listTopics: TopicsWithPartitions = auxConsumer.listTopics.asScala.toMap mapValues (_.asScala.toList) having { topics =>
    log.info(s"${topics.size} topics fetched for connection [$name]")
  }

  /**
   * Returns the list of partitions for the specified topic name.
   *
   * @param topic the name of the topic to retrieve partitions for.
   * @return the list of partition information records.
   */
  def partitionsFor(topic: String): List[PartitionInfo] = auxConsumer.partitionsFor(topic).asScala.toList having { parts =>
    log.info(s"${parts.size} partition(s) fetched for topic [$topic], connection [$name]")
  }

  /**
   * Sends a message with an optional key to a Kafka topic and returns a future containing the response.
   *
   * @param topic     the name of the topic to send the message to.
   * @param key       optional message key.
   * @param partition optional topic partition number.
   * @param timestamp optional timestamp to associate with the message.
   * @param options   additional properties, compatible with KafkaProducer settings.
   * @param flush     whether to flush and close the producer before returning. In this case the returned
   *                  Future will be complete.
   * @return the future containing the metadata of the inserted record.
   */
  def publish[K >: Null: Serializer, V: Serializer](topic: String, key: Option[K], value: V,
                                                    partition: Option[Int] = None, timestamp: Option[Long] = None,
                                                    options: Map[String, String] = Map.empty,
                                                    flush: Boolean = false): Future[RecordMetadata] = {
    val producer = createProducer[K, V](options)
    val target = Destination(topic, partition)
    val frmd = sendWithProducer(producer)(target)(key, value, timestamp)
    log.info(s"A message published onto [$target] by connection [$name]")

    if (flush) {
      producer.flush
      producer.close
    }

    frmd
  }

  /**
   * Creates a Kafka producer based on the default settings and additional option overrides.
   */
  private def createProducer[K: Serializer, V: Serializer](options: Map[String, String]) = {
    val kser = implicitly[Serializer[K]]
    val vser = implicitly[Serializer[V]]

    val config = ConfigFactory.parseMap(options.asJava).withFallback(PRODUCER_CONFIG)
    val conf = KafkaProducer.Conf(PRODUCER_CONFIG, kser, vser).withProperty(BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    KafkaProducer(conf)
  }

  /**
   * Sends a message to a certain destination using the supplied producer.
   */
  private def sendWithProducer[K >: Null: Serializer, V: Serializer](
    producer: KafkaProducer[K, V])(target: Destination)(key: Option[K], value: V, timestamp: Option[Long]) = {
    val record = KafkaProducerRecord(target, key, value, timestamp)
    producer.send(record)
  }
}