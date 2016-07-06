package org.schedoscope.dsl.flink

import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.Transformation
import org.apache.flink.api.scala._

case class FlinkTransformation(transformation: () => DataSet[DynamicView]) extends Transformation {

  import FlinkTransformation._

  /**
    * Name of the transformation type.
    */
  override def name: String = "flink"
}

object FlinkTransformation {

  implicit def viewToDataSet(view: View) : DataSet[DynamicView] = {
    val flinkEnv = ExecutionEnvironment.getExecutionEnvironment
    val input = ViewInputFormat(view).asFlinkTuples()
    flinkEnv.createInput(input)
  }

}