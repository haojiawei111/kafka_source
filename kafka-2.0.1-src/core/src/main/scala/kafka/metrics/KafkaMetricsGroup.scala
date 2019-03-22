
package kafka.metrics

import java.util.concurrent.TimeUnit

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.utils.Logging
import org.apache.kafka.common.utils.Sanitizer

// 监控组
trait KafkaMetricsGroup extends Logging {

  /**
   * Creates a new MetricName object for gauges, meters, etc. created for this
   * metrics group.  为此度量标准组创建的仪表，仪表等创建新的MetricName对象。
   * @param name Descriptive name of the metric.
   * @param tags Additional attributes which mBean will have.
   * @return Sanitized metric name object.
   */
  def metricName(name: String, tags: scala.collection.Map[String, String]): MetricName = {
    val klass = this.getClass // 类的全类名
    val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName  //包名
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "") //类的简写名称

    explicitMetricName(pkg, simpleName, name, tags)
  }


  protected def explicitMetricName(group: String, typeName: String, name: String,tags: scala.collection.Map[String, String]): MetricName = {

    val nameBuilder: StringBuilder = new StringBuilder

    nameBuilder.append(group)

    nameBuilder.append(":type=")

    nameBuilder.append(typeName)

    if (name.length > 0) {
      nameBuilder.append(",name=")
      nameBuilder.append(name)
    }

    // 把tags的key和value拼接起来
    val scope: String = toScope(tags).getOrElse(null)
    // 把tags的key和value拼接起来
    val tagsName = toMBeanName(tags)

    //添加到nameBuilder中
    tagsName.foreach(nameBuilder.append(",").append(_))

    new MetricName(group, typeName, name, scope, nameBuilder.toString)

  }

  def newGauge[T](name: String, metric: Gauge[T], tags: scala.collection.Map[String, String] = Map.empty) =
    Metrics.defaultRegistry().newGauge(metricName(name, tags), metric)

  def newMeter(name: String, eventType: String, timeUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty) =
    Metrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit)

  def newHistogram(name: String, biased: Boolean = true, tags: scala.collection.Map[String, String] = Map.empty) =
    Metrics.defaultRegistry().newHistogram(metricName(name, tags), biased)

  def newTimer(name: String, durationUnit: TimeUnit, rateUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty) =
    Metrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit)

  def removeMetric(name: String, tags: scala.collection.Map[String, String] = Map.empty) =
    Metrics.defaultRegistry().removeMetric(metricName(name, tags))

  private def toMBeanName(tags: collection.Map[String, String]): Option[String] = {
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != "" }
    if (filteredTags.nonEmpty) {
      val tagsString = filteredTags.map { case (key, value) => "%s=%s".format(key, Sanitizer.jmxSanitize(value)) }.mkString(",")
      Some(tagsString)
    }
    else None
  }

  private def toScope(tags: collection.Map[String, String]): Option[String] = {
    // 过滤tagValue == '' 的值
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != ""}
    if (filteredTags.nonEmpty) {
      // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
      val tagsString = filteredTags
        .toList.sortWith((t1, t2) => t1._1 < t2._1)
        .map { case (key, value) => "%s.%s".format(key, value.replaceAll("\\.", "_"))}
        .mkString(".")

      Some(tagsString)
    }
    else None
  }

}

object KafkaMetricsGroup extends KafkaMetricsGroup
