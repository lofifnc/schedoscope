package org.schedoscope.dsl.flink


import org.schedoscope.dsl.{Field, View}

import scala.collection.mutable
import scala.language.dynamics

class DynamicView(val fields: Seq[SimpleField[_]]) extends Dynamic {

//  println(flds.length)
  val values = mutable.HashMap.empty[SimpleField[_], Any]
  fields.foreach(f => values.put(f, None))


  //      def selectDynamic(name: String) = {
  //        fields.find(_.n == name) match {
  //         case Some(f: Field[Any]) =>
  //            get(f)
  //          case None =>
  //            None
  //        }
  //      }

  def updateDynamic[T](name: String)(value: T) = {
    println(name)
    fields.find(_.n == name) match {
      case Some(f: SimpleField[Any]) =>
        set(f, value)
      case None =>
    }
  }

  def get[T](field: Field[T]): Option[T] = {
    val f = SimpleField[T](field)
    if(values.contains(f)) {
      Some(values.get(f).get.asInstanceOf[T])
    }else{
      None
    }
  }

  def set[T](f: SimpleField[T], value: T) = {
    if (values.contains(f)) {
      values.put(f, value)
    } else {
      throw new IllegalArgumentException(s"Field ${f.n} not in view!")
    }
  }

  def put(f: SimpleField[_], value: Any) = {
    if (values.contains(f)) {
      values.put(f, value)
    } else {
      throw new IllegalArgumentException(s"Field ${f.n} not in view!")
    }
  }

  def set[T](field: Field[T], value: T): Unit = {
    val f = SimpleField(field)
    set(f, value)
  }

  override def toString(): String = {
    values.map(f => f._1.n + " : " + f._2).mkString("; ")
  }
}

object DynamicView {
  def apply(view: View): DynamicView = {
    new DynamicView(view.fields.map{ f => SimpleField(f.n,f.t) })
  }
}
