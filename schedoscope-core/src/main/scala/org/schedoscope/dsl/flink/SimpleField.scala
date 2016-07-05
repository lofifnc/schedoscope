package org.schedoscope.dsl.flink

import org.schedoscope.dsl.Field

case class SimpleField[T](n: String, t: Manifest[T])

object SimpleField {
  def apply[T](f: Field[T]) : SimpleField[T] = {
    SimpleField(f.n,f.t)
  }
}