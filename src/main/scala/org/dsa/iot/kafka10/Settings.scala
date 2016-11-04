package org.dsa.iot.kafka10

import com.typesafe.config.ConfigFactory

/**
 * Kafka DSLink settings.
 */
object Settings {
  private lazy val config = ConfigFactory.load

  val DEFAULT_HOST = config.getString("defaultHost")
  val DEFAULT_PORT = config.getInt("defaultPort")
  val DEFAULT_BROKER_URL = DEFAULT_HOST + ":" + DEFAULT_PORT

  val AUX_GROUP_ID = config.getString("auxGroupId")

  val ACTION_RETRIES = config.getInt("actionRetries")
  
  val PRODUCER_CONFIG = config.getConfig("producer")
  val CONSUMER_CONFIG = config.getConfig("consumer")
}