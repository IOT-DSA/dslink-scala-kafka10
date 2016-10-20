package org.dsa.iot.kafka10

import scala.collection.JavaConverters.{ asScalaBufferConverter, mapAsScalaMapConverter }

import org.apache.kafka.common.serialization.StringDeserializer
import org.dsa.iot.scala.Having
import org.slf4j.LoggerFactory

import cakesolutions.kafka.KafkaConsumer

/**
 * Encapsulates Kafka connection info and operations.
 */
case class KafkaConnection(name: String, brokerUrl: String) {

  private val log = LoggerFactory.getLogger(getClass)

  private lazy val auxConsumer = {
    val keyDSer = new StringDeserializer
    val valDSer = new StringDeserializer
    val conf = KafkaConsumer.Conf(keyDSer, valDSer, brokerUrl, Settings.AUX_GROUP_ID)
    KafkaConsumer(conf)
  }

  def close() = {
    auxConsumer.close
    log.info("Aux consumer closed")
  }

  def listTopics = auxConsumer.listTopics.asScala.toMap mapValues (_.asScala.toList) having { topics =>
    log.info(topics.size + " topics fetched")
  }

  def partitionsFor(topic: String) = auxConsumer.partitionsFor(topic).asScala.toList having { parts =>
    log.info(parts.size + " partition(s) fetched for topic [" + topic + "]")
  }
}