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

// me.valik.osm.export.JobMetrics
package me.valik.osm.export

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Gauge

import me.valik.spark.metrics.RegistryInstrumentation

case class JobMetrics(totalCount: Long, status: JobStatus)

object JobMetrics {
  implicit val instrumentation: RegistryInstrumentation[JobMetrics] =
    new RegistryInstrumentation[JobMetrics] {
      override def register(registry: MetricRegistry, obj: JobMetrics): Unit = {
        registry.counter(metric("total_count")).inc(obj.totalCount)
        registry.register(metric("status"), new Gauge[String]{
          override def getValue: String = s"${obj.status.name}: ${obj.status.reason}"
        })
      }

      private def metric(name: String, names: String*) = MetricRegistry.name("export.stats", name +: names: _*)
    }
}

case class JobStatus(name: String, reason: Option[String] = None)

object JobStatus {
  abstract class FAIL(failReason: String) extends JobStatus("FAIL", Some(failReason))
  object OK extends JobStatus("OK")
}
