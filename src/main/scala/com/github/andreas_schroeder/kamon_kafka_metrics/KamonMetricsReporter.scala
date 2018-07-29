package com.github.andreas_schroeder.kamon_kafka_metrics

import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import com.github.andreas_schroeder.kamon_kafka_metrics.KamonMetricsReporter._
import kamon.Kamon
import kamon.metric.{Counter, Gauge}
import org.apache.kafka.common.metrics.stats.Total
import org.apache.kafka.common.metrics.{KafkaMetric, MetricsReporter}

import scala.concurrent.duration._
import scala.util.{Success, Try}

class KamonMetricsReporter extends MetricsReporter {

  val metricBlacklist = Set("commit-id", "version")

  val metrics: AtomicReference[List[MetricBridge]] = new AtomicReference(List.empty)

  def add(metric: KafkaMetric): Unit = {
    val mn = metric.metricName
    if (!metricBlacklist.contains(mn.name)) {
      val tags       = mn.tags
      val metricName = kamonName(metric)
      val bridge =
        if (countMetric(metric))
          new CounterBridge(metric, Kamon.counter(metricName).refine(tags))
        else
          new GaugeBridge(metric, Kamon.gauge(metricName).refine(tags), factor(metricName))

      metrics.getAndUpdate((l: List[MetricBridge]) => bridge +: l)
    }
  }

  override def init(metrics: util.List[KafkaMetric]): Unit = metrics.forEach(add)

  override def metricChange(metric: KafkaMetric): Unit = {
    metricRemoval(metric)
    add(metric)
  }

  override def metricRemoval(metric: KafkaMetric): Unit = {
    metrics.getAndUpdate((l: List[MetricBridge]) => l.filterNot(_.kafkaMetric == metric))
    if (countMetric(metric)) {
      Kamon.counter(kamonName(metric)).remove(metric.metricName.tags)
    } else {
      Kamon.gauge(kamonName(metric)).remove(metric.metricName.tags)
    }
  }

  def kamonName(metric: KafkaMetric): String = {
    val mn = metric.metricName
    // adjust for Prometheus convention
    s"${mn.group}_${mn.name}".replaceAll("-", "_") match {
      case n if n.endsWith("_rate")  => n + "_per_ns"
      case n if n.endsWith("_ratio") => n + "_per_mil"
      case n                         => n
    }
  }

  def factor(metricName: String): Double =
    if (metricName.endsWith("_per_ns")) nanosecondsFactor
    else if (metricName.endsWith("_per_mil")) permilFactor
    else 1.0

  def countMetric(metric: KafkaMetric): Boolean = Try(metric.measurable()) match {
    case Success(_: Total) => true
    case _                 => false
  }

  override def close(): Unit = updater.get().foreach(_.cancel(true))

  override def configure(configs: util.Map[String, _]): Unit = {
    val interval =
      Option(configs.get(reportInterval)).flatMap(v => Try(v.toString.toInt).toOption).getOrElse(defaultReportInterval)
    updater.set(Some(Kamon.scheduler().scheduleAtFixedRate(doUpdate, interval, interval, TimeUnit.MILLISECONDS)))
  }

  private val updater: AtomicReference[Option[ScheduledFuture[_]]] = new AtomicReference(None)

  private val doUpdate: Runnable = () => metrics.get().foreach(_.update())

  private val nanosecondsFactor: Double = 1.0 * 1.second.toNanos

  private val permilFactor: Double = 1000.0

  KamonMetricsReporter.instance.set(this)
}

object KamonMetricsReporter {

  val reportInterval = "kamon.reporter.interval.ms"

  val defaultReportInterval: Long = 1.second.toMillis

  /**
    * For testing purposes.
    */
  val instance: AtomicReference[KamonMetricsReporter] = new AtomicReference[KamonMetricsReporter]()

  trait MetricBridge {
    val kafkaMetric: KafkaMetric
    def metricValue: Long
    def update(): Unit
  }

  class GaugeBridge(val kafkaMetric: KafkaMetric, kamonMetric: Gauge, factor: Double = 1.0) extends MetricBridge {

    override def metricValue: Long = kafkaMetric.metricValue match {
      case d: java.lang.Double => (d * factor).toLong
      case _                   => 0L
    }

    override def update(): Unit = kamonMetric.set(metricValue)
  }

  class CounterBridge(val kafkaMetric: KafkaMetric, kamonMetric: Counter) extends MetricBridge {

    var last: Long = 0

    override def metricValue: Long = kafkaMetric.metricValue match {
      case d: java.lang.Double => d.toLong
      case _                   => 0L
    }

    override def update(): Unit = {
      val newValue = metricValue
      kamonMetric.increment(newValue - last)
      last = newValue
    }
  }
}
