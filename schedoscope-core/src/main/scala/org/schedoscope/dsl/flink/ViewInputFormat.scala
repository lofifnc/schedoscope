package org.schedoscope.dsl.flink

import java.util.Date

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.configuration
import org.apache.flink.hcatalog.HCatInputFormatBase
import org.apache.hadoop.conf.Configuration
import org.apache.hive.hcatalog.data.HCatRecord
import org.apache.hive.hcatalog.data.schema.HCatSchema
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.schedoscope.dsl.{Structure, View}

class ViewInputFormat(val fields: Seq[SimpleField[_]],
                      database: String,
                      table: String,
                      config: Configuration
                     ) extends HCatInputFormatBase[DynamicView](database, table, config) {

  import ViewInputFormat._

  var vals: Array[Any] = Array[Any]()

  override def configure(parameters: configuration.Configuration): Unit = {
    //    super.configure(parameters)
    //    vals = new Array[Any](fieldNames.length)
  }


  override def buildFlinkTuple(t: DynamicView, record: HCatRecord): DynamicView = {
    var i: Int = 0
    val view = new DynamicView(fields)

    println(this.fieldNames.mkString(", "))

    this.fieldNames.foreach { name =>
      val field = fields.find(f => f.n == name).get
      outputSchema.get(i).getTypeString
      val value = deserializeField(name, field.t, record, this.outputSchema)
      view.put(field, value)
      i += 1
    }

    view
  }

  override def getMaxFlinkTupleSize: Int = 100


}

object ViewInputFormat {

  def deserializeField[T](name: String, t: Manifest[T],
                          record: HCatRecord,
                          schema: HCatSchema): Any = {
    //    if (v == null || "null".equals(v)) {
    //      return v
    //    }
    if (t == manifest[Int])
      record.getInteger(name, schema)
    else if (t == manifest[Long])
      record.getLong(name, schema)
    else if (t == manifest[Byte])
      record.getByte(name, schema)
    else if (t == manifest[Boolean])
      record.getBoolean(name, schema)
    else if (t == manifest[Double])
      record.getBoolean(name, schema)
    else if (t == manifest[Float])
      v.asInstanceOf[String].toFloat
    else if (t == manifest[String])
      v.asInstanceOf[String]
    else if (t == manifest[Date])
      v.asInstanceOf[String] // TODO: parse date?
    else if (classOf[Structure].isAssignableFrom(t.runtimeClass)) {
      // Structures are given like [FieldValue1,FieldValue2,...]
      // Maps are given las json
      implicit val format = DefaultFormats
      val parsed = parse(v.toString())
      parsed.extract[Map[String, _]]
    } else if (t.runtimeClass == classOf[List[_]]) {
      // Lists are given like [el1, el2, ...]
      implicit val format = DefaultFormats
      val parsed = parse(v.toString())
      parsed.extract[List[_]]
    } else if (t.runtimeClass == classOf[Map[_, _]]) {
      // Maps are given las json
      implicit val format = DefaultFormats
      val parsed = parse(v.toString())
      parsed.extract[Map[String, _]]
    } else throw new RuntimeException("Could not deserialize field of type " + t + " with value " + v)
  }

  def apply(view: View): ViewInputFormat = {
    val config = new Configuration()

    //    val gen = TupleGeneric[T]

    //    val product = gen.to(view.asInstanceOf[T])

    println(view.tableName)
    val dbName = view.tableName
    val tableName = dbName.split("\\.")(1)
    val databaseName = dbName.split("\\.")(0)

    new ViewInputFormat(
      view.fields.map { f => SimpleField(f.n, f.t) },
      "test_schedoscope_example_osm_datahub",
      tableName,
      config)
  }

  private[flink] def fieldNames2Indices(
                                         typeInfo: TypeInformation[_],
                                         fields: Array[String]): Array[Int] = {
    typeInfo match {
      case ti: CaseClassTypeInfo[_] =>
        val result = ti.getFieldIndices(fields)

        if (result.contains(-1)) {
          throw new IllegalArgumentException("Fields '" + fields.mkString(", ") +
            "' are not valid for '" + ti.toString + "'.")
        }

        result

      case _ =>
        throw new UnsupportedOperationException("Specifying fields by name is only" +
          "supported on Case Classes (for now).")
    }
  }
}

