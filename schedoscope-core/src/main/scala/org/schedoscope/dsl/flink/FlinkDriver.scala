package org.schedoscope.dsl.flink

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.joda.time.LocalDateTime
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.Transformation._
import org.schedoscope.scheduler.driver.{DriverRunSucceeded, DriverRunState, DriverRunHandle, DriverOnBlockingApi}

import scala.concurrent.Future

/**
  * Created by akolb on 05.07.16.
  */
class FlinkDriver(val driverRunCompletionHandlerClassNames: List[String], executionEnvironment: ExecutionEnvironment) extends DriverOnBlockingApi[FlinkTransformation]{
  /**
    * The name of the transformations executed by this driver. Must be equal to t.name for any t: T.
    */
  override def transformationName: String = "hive"

  /**
    * Create a driver run, i.e., start the execution of the transformation asychronously.
    */
  override def run(t: FlinkTransformation): DriverRunHandle[FlinkTransformation] = {
    new DriverRunHandle[FlinkTransformation](this, new LocalDateTime(), t, Future {
      executeFlinkJob(t.transformation)
    })
  }

  def executeFlinkJob(transformation: () => DataSet[DynamicView]): DriverRunState[FlinkTransformation] = {
    val trans = transformation.apply()
    val executionEnvironment = trans.getExecutionEnvironment

    trans.print()

    executionEnvironment.execute()

    new DriverRunSucceeded[FlinkTransformation](this, "test")
  }
}
