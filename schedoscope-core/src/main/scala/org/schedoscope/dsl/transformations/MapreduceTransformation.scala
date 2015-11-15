/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.dsl.transformations

import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.Job
import org.schedoscope.Settings
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.mapreduce.MRJobConfig
import java.net.URI
import org.schedoscope.Schedoscope
import org.schedoscope.dsl.View
import scala.collection.mutable.ListBuffer

/**
 *  Compute a view using a plain Map-Reduce job.
 *
 *  @param view reference to the view being computed
 *  @param createJob closure to create the job configuration
 *  @param dirsToDelete List of directories to empty before execution. Includes the view's fullPath
 *
 */
case class MapreduceTransformation(v: View, createJob: (Map[String, Any]) => Job, dirsToDelete: List[String] = List()) extends Transformation {

  override def name = "mapreduce"

  lazy val job = createJob(configuration.toMap)

  val directoriesToDelete = dirsToDelete ++ List(v.fullPath)

  description = StringUtils.abbreviate(v.urlPath, 100)

  override def fileResourcesToChecksum = {
    val jarName = try {
      job.getConfiguration().get(MRJobConfig.JAR).split("/").last
    } catch {
      case _: Throwable => null
    }

    Schedoscope.settings
      .getDriverSettings("mapreduce")
      .libJarsHdfs
      .filter(lj => jarName == null || lj.contains(jarName))
  }

  def configure() {
    // if job jar hasn't been registered, add all mapreduce libjars
    // to distributed cache
    if (job.getConfiguration().get(MRJobConfig.JAR) == null) {
      fileResourcesToChecksum.foreach(r => {
        try {
          job.addCacheFile(new URI(r))
        } catch {
          case _: Throwable => Unit
        }
      })
    }
    configuration.foreach(c => job.getConfiguration.set(c._1, c._2.toString))
  }
}
