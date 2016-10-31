package org.dsa.iot.kafka10

import scala.concurrent.Future

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.Serializer

/**
 * Encapsulates Kafka topic info and operations.
 */
case class KafkaTopic(name: String, conn: KafkaConnection) {

  /**
   * Returns the list of partitions for this topic.
   *
   * @return the list of partition information records.
   */
  def partitions: List[PartitionInfo] = conn.partitionsFor(name)

  /**
   * Sends a message with an optional key to the Kafka topic and returns a future containing the response.
   *
   * @param key       optional message key.
   * @param partition optional topic partition number.
   * @param timestamp optional timestamp to associate with the message.
   * @param options   additional properties, compatible with KafkaProducer settings.
   * @param flush     whether to flush and close the producer before returning. In this case the returned
   *                  Future will be complete.
   * @return the future containing the metadata of the inserted record.
   */
  def publish[K >: Null: Serializer, V: Serializer](key: Option[K], value: V,
                                                    partition: Option[Int] = None, timestamp: Option[Long] = None,
                                                    options: Map[String, String] = Map.empty,
                                                    flush: Boolean = false): Future[RecordMetadata] =
    conn.publish(name, key, value, partition, timestamp, options, flush)
}