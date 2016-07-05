package org.schedoscope.dsl.flink

import org.schedoscope.dsl.transformations.Transformation

case class FlinkTransformation(transformation: () => _) extends Transformation {
  /**
    * Name of the transformation type.
    */
  override def name: String = "flink"
}
