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

// me.valik.osm.export.OsmExportJob
package me.valik.osm.export

import me.valik.spark.SparkApp
import io.circe.generic.auto._

/**
  * spark-submit job
  */
object OsmExportJob extends SparkApp {

  run {
    case args => implicit spark =>
      // run job
      val metrics: JobMetrics = ???

      // promote and save job metrics
      stats.report(metrics)
      stats.write("/tmp/job-output")
  }
}
