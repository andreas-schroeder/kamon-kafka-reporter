# Kamon Kafka Metric Reporter

[![Build Status](https://travis-ci.org/andreas-schroeder/redisks.svg?branch=master)](https://travis-ci.org/andreas-schroeder/redisks)
[![Download](https://api.bintray.com/packages/and-schroeder/maven/redisks/images/download.svg) ](https://bintray.com/and-schroeder/maven/redisks/_latestVersion)

MetricReporter that forwards Kafka metrics to Kamon.

## Why

When having Kamon set up for exposing metrics to Prometheus, the valuable Kafka client metrics can to be published
trough that channel as well instead of bringing up another web server.

## How to use

### Gradle

```groovy
repositories {
   jcenter()
}

dependencies {
    compile group: 'com.github.andreas-schroeder', name: 'kamon-kafka-reporter_2.12', version: '0.0.0'
}
```

### sbt

```scala

resolvers += Resolver.bintrayRepo("and-schroeder", "maven")

libraryDependencies += "com.github.andreas-schroeder" %% "kamon-kafka-reporter" % "0.0.0"
```

### Configuring the Metrics Reporter

Through config:
```
metric.reporters=com.github.andreas_schroeder.kamon_kafka_metrics.KamonMetricsReporter
```

Through code:
```scala
import java.util.Properties
import org.apache.kafka.streams.StreamsConfig
import com.github.andreas_schroeder.kamon_kafka_metrics.KamonMetricsReporter

val props = new Properties
props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, classOf[KamonMetricsReporter].getName)
```

