package org.dsa.iot.kafka10

import java.net.InetSocketAddress

import org.apache.kafka.common.config.ConfigException
import org.scalatest.{ Finders, Matchers, Suite, WordSpecLike }

/**
 * Kafka10 package test suite.
 */
class PackageSpec extends Suite with WordSpecLike with Matchers {
  import Settings._

  "parseBrokerList" should {
    "parse valid hosts with ports" in {
      parseBrokerList("yahoo.com:1234, google.com:80,apple.com:8080") shouldBe
        isa("yahoo.com", 1234) :: isa("google.com", 80) :: isa("apple.com", 8080) :: Nil
    }
    "parse valid hosts without ports" in {
      parseBrokerList("yahoo.com, google.com") shouldBe
        isa("yahoo.com", DEFAULT_PORT) :: isa("google.com", DEFAULT_PORT) :: Nil
    }
    "parse mixed hosts with/without ports" in {
      parseBrokerList("yahoo.com:80, google.com,apple.com:99") shouldBe
        isa("yahoo.com", 80) :: isa("google.com", DEFAULT_PORT) :: isa("apple.com", 99) :: Nil
    }
    "fail on a blank string" in {
      a[ConfigException] should be thrownBy parseBrokerList(" ")
    }
    "fail on a string containing empty addresses" in {
      a[ConfigException] should be thrownBy parseBrokerList(", ")
    }
    "fail on unresolved host name" in {
      a[ConfigException] should be thrownBy parseBrokerList("aaaaaaaaaaaaaaaa:123")
    }
    "fail on bad port format" in {
      a[ConfigException] should be thrownBy parseBrokerList("google.com:port")
    }
    "fail on invalid port number" in {
      a[IllegalArgumentException] should be thrownBy parseBrokerList("google.com:9999999")
    }
  }

  private def isa(host: String, port: Int) = new InetSocketAddress(host, port)
}