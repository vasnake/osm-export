/*
 * Copyright 2019 Valentin Fedulov <vasnake@gmail.com>
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

// me.valik.spark.SparkApp
package me.valik.spark

import nl.grons.metrics4.scala.InstrumentedBuilder

/**
  * App wrapper for spark job with custom job metrics
  */
trait SparkApp extends App with InstrumentedBuilder {
  import nl.grons.metrics4.scala.MetricName
  import com.codahale.metrics.MetricRegistry

  import org.apache.spark.SparkConf
  import org.apache.spark.deploy.SparkHadoopUtil
  import org.apache.spark.sql.SparkSession

  import me.valik.spark.metrics.Instrumentation

  type Job[R] = SparkSession => R

  override lazy val metricBaseName = MetricName("job.stats")
  override val metricRegistry = new MetricRegistry()
  val jobName = getClass.getSimpleName.stripSuffix("$")

  /**
    * job metrics handler, use `report` and `write` methods after job finishes it's job
    * {{{
    *       stats.report(jobMetrics)
    *       stats.write(outputPath)
    * }}}
    */
  val stats: JobStats = new JobStats(s"$jobName.${metricBaseName.name}")
  val timer = metrics.timer("time")

  /**
    * Spark job main program
    *
    * @param handler your function, presumably in form of {{{
      run { case inputPath :: outputPath :: Nil => implicit spark => ??? }
      }}}
    * @tparam R return type of your program
    */
  def run[R](handler: PartialFunction[List[String], Job[R]]): Unit = {
    val job = handler.applyOrElse(args.toList, { wrong: List[String] =>
      throw new IllegalArgumentException(s"Wrong parameters (${wrong.length}):\n"
        + s"${wrong.map("'" + _ + "'").mkString("\n")}")
    })

    // spark-submit conf
    val conf = new SparkConf()
    // local app (standalone) conf
    // val conf = new SparkConf().setMaster("local").setAppName("my awesome app")

    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    // There is some bug with custom hadoop properties in the yarn cluster mode
    import scala.collection.JavaConverters._
    spark.sparkContext.hadoopConfiguration.asScala.foreach { e =>
      SparkHadoopUtil.get.conf.set(e.getKey, e.getValue)
    }

    try {
      Instrumentation.register(jobName, this)
      timer.time(job(spark))
    } finally {
      spark.stop()
    }
  }
}

class JobStats(name: String) {
  import io.circe._
  import io.circe.syntax._
  import org.apache.commons.lang3.StringUtils
  import org.apache.hadoop.fs.{FileSystem, Path}
  import scala.collection.mutable

  import me.valik.spark.metrics.Instrumentation

  import org.apache.spark.SparkContext

  private val jsonStats = mutable.ArrayBuffer.empty[(String, Json)]

  def report[T: Instrumentation : Encoder](metrics: T): Unit = {
    Instrumentation.register(name, metrics)
    jsonStats += (StringUtils.uncapitalize(metrics.getClass.getSimpleName) -> metrics.asJson)
  }

  def write(outputPath: String)(implicit sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val os = fs.create(new Path(outputPath + "/_STATS.json"))
    try {
      os.write(toJson.getBytes("UTF-8"))
    } finally {
      os.close()
    }
  }

  private def toJson = Json.obj(jsonStats: _*).spaces4
}
