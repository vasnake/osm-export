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

// org.apache.spark.metrics.CustomSource
package org.apache.spark.metrics

import org.apache.spark.metrics.source.Source

trait CustomSource extends Source

/**
  * register custom spark metric source
  */
object CustomSource {
  import org.apache.spark.SparkEnv

  /**
    * register metric source in SparkEnv `metricSystem`
    * @param source
    */
  def register(source: Source): Unit = {
    val env = SparkEnv.get
    if (env == null) throw new IllegalStateException(s"Can't register metrics source '${source.sourceName}'"
      + " because SparkEnv is not set yet")

    env.metricsSystem.registerSource(source)
  }
}
