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

// me.valik.spark.metrics.Instrumentation
package me.valik.spark.metrics

import com.codahale.metrics.MetricSet
import com.codahale.metrics.MetricRegistry
import nl.grons.metrics4.scala.InstrumentedBuilder

trait Instrumentation[T] {
  def metrics(obj: T): MetricSet
}

trait DefaultInstrumentations {
  implicit def builder[T <: InstrumentedBuilder]: Instrumentation[T] = new Instrumentation[T] {
    override def metrics(obj: T): MetricSet = obj.metricRegistry
  }
}

/**
  * link metrics to Spark registry
  */
object Instrumentation extends DefaultInstrumentations {
  // spark custom metric source
  import org.apache.spark.metrics.CustomSource

  def metrics[T](obj: T)(implicit instrument: Instrumentation[T]): MetricSet = instrument.metrics(obj)

  def source[T: Instrumentation](name: String, obj: T): CustomSource = new CustomSource {
    def sourceName: String = name

    def metricRegistry: MetricRegistry = metrics(obj) match {
      case registry: MetricRegistry => registry
      case set =>
        val registry = new MetricRegistry()
        registry.registerAll(set)
        registry
    }
  }

  def register[T: Instrumentation](name: String, obj: T): Unit = CustomSource.register(source(name, obj))
}
