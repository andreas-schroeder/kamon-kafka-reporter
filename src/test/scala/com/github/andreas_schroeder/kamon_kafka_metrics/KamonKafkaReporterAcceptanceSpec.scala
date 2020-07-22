package com.github.andreas_schroeder.kamon_kafka_metrics

import java.net.ServerSocket
import java.nio.file.Files
import java.util.Properties

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FeatureSpec, GivenWhenThen, MustMatchers}

import scala.collection.JavaConverters._

class KamonKafkaReporterAcceptanceSpec
  extends FeatureSpec
    with MustMatchers
    with GivenWhenThen
    with EmbeddedKafka
    with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(200, Millis)))

  feature("Metrics") {

    scenario("Running Kafka App") {
      withRunningKafka {
        createTopics("topic-one", "topic-two")
        withStreamsApp(_.stream[String, String]("topic-one").to("topic-two")) { streams =>
          send("topic-one", "key" -> "message-1", "key" -> "message-2")
          eventually { streams.allMetadata().isEmpty mustBe false }
          val updates = KamonMetricsReporter.instance.get().metrics
          eventually { updates.get().size mustBe 116 }
          eventually {
            val metric = updates.get().find(_.kafkaMetric.metricName.name.contains("records-consumed-total")).get
            metric.metricValue mustBe 2L
          }
        }
      }
    }
  }

  private def streamsConfig = {
    val port  = embeddedKafkaConfig.kafkaPort
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-app")
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "join-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:$port")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "1000")
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8000")
    props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, classOf[KamonMetricsReporter].getName)
    props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kafka-streams").toString)
    props
  }

  private def withStreamsApp(app: StreamsBuilder => Unit)(body: KafkaStreams => Any): Any = {
    val builder = new StreamsBuilder
    app(builder)
    val streams = new KafkaStreams(builder.build(), streamsConfig)
    streams.start()
    try {
      body(streams)
    } finally {
      streams.close()
    }
  }

  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(freePort, freePort, Map("offsets.topic.replication.factor" -> "1"), Map.empty, Map.empty)

  implicit val stringSerializer: StringSerializer = new StringSerializer

  implicit val stringDeserializer: StringDeserializer = new StringDeserializer

  private def createTopics(topicNames: String*): Unit =
    topicNames.foreach(name => createCustomTopic(name, partitions = 2))

  private def listTopics(): Set[String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + embeddedKafkaConfig.kafkaPort)
    val consumer = new KafkaConsumer[String, String](props, stringDeserializer, stringDeserializer)
    val topics   = consumer.listTopics().asScala.keys.toSet
    consumer.close()
    topics
  }

  private def send(topic: String, records: (String, String)*): Unit =
    for ((key, value) <- records) {
      publishToKafka(topic, key, value)
    }

  private def freePort = {
    val socket = new ServerSocket(0)
    val port   = socket.getLocalPort
    socket.close()
    port
  }
}
