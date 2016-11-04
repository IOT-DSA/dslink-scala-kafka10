package org.dsa.iot.kafka10

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord }
import org.apache.kafka.common.serialization.Deserializer
import org.dsa.iot.scala.Having
import org.slf4j.LoggerFactory

import com.typesafe.config.ConfigFactory

import cakesolutions.kafka.KafkaConsumer
import rx.lang.scala.Observable
import rx.lang.scala.subjects.BehaviorSubject

/**
 * Implements a basic subscription for one topic. Does not allow partition assignment,
 * offset manupulation etc.
 */
class BasicSubscription[K: Deserializer, V: Deserializer](
    name: String, groupId: String, options: Map[String, String], topic: KafkaTopic) {

  import Settings._

  private val log = LoggerFactory.getLogger(getClass)

  private val subj = BehaviorSubject[ConsumerRecord[K, V]]()

  private val started = new AtomicBoolean(false)
  private var thread: Thread = null

  private lazy val consumer = createConsumer(groupId, options) having
    log.info(s"A consumer created for groupId [$groupId]")

  private val runnable = new Runnable {
    def run() = while (isStarted) {
      val records = consumer.poll(2000).asScala
      log.debug(s"${records.size} messages downloaded")
      records foreach subj.onNext
    }
  }

  /**
   * The message stream from Kafka.
   */
  val output: Observable[ConsumerRecord[K, V]] = subj

  /**
   * Checks if the subscription is currently active.
   */
  def isStarted = started.get

  /**
   * Starts the subscription.
   */
  def start() = synchronized {
    if (!isStarted) {
      consumer.subscribe(List(topic.name).asJava)
      log.info(s"Consumer subscribed to topic [${topic.name}]")
      thread = new Thread(runnable)
      thread.start
      started.set(true)
      log.info(s"Subscription [$name] for topic [${topic.name}] started")
    } else
      log.warn(s"The subscription [$name] has already been started")
  }

  /**
   * Stops the subscription.
   */
  def stop() = synchronized {
    if (isStarted) {
      started.set(false) having log.info(s"Subscription [$name] for topic [${topic.name}] stopped")
      thread.join
    } else
      log.warn(s"The subscription [$name] has not been started")
  }

  /**
   * Stops the subscription, if started; closes the consumer.
   */
  def close() = synchronized {
    if (isStarted)
      stop
    consumer.close
    log.info(s"Subscription [$name] for topic [${topic.name}] closed")
  }

  /**
   * Creates a Kafka consumer based on the default settings and additional option overrides.
   */
  private def createConsumer(groupId: String, options: Map[String, String]) = {
    import org.apache.kafka.clients.consumer.ConsumerConfig._

    val kdes = implicitly[Deserializer[K]]
    val vdes = implicitly[Deserializer[V]]

    val config = ConfigFactory.parseMap(options.asJava).withFallback(CONSUMER_CONFIG)
    val conf = KafkaConsumer.Conf(CONSUMER_CONFIG, kdes, vdes)
      .withProperty(BOOTSTRAP_SERVERS_CONFIG, topic.conn.brokerUrl)
      .withProperty(GROUP_ID_CONFIG, groupId)
    KafkaConsumer(conf)
  }
}