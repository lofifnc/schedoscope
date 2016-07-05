package org.schedoscope.dsl.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.configuration
import org.apache.flink.hcatalog.HCatInputFormatBase
import org.apache.hadoop.conf.Configuration
import org.apache.hive.hcatalog.data.HCatRecord
import org.schedoscope.dsl.View
import org.schedoscope.dsl.Field
import org.schedoscope.test.ViewSerDe

class ViewInputFormat(val fields: Seq[SimpleField[_]],
                      database: String,
                                    table: String,
                                    config: Configuration
                                   ) extends HCatInputFormatBase[DynamicView](database, table, config) {

  var vals: Array[Any] = Array[Any]()

  override def configure(parameters: configuration.Configuration): Unit = {
//    super.configure(parameters)
//    vals = new Array[Any](fieldNames.length)
  }


  override def buildFlinkTuple(t: DynamicView, record: HCatRecord): DynamicView = {
    var i: Int = 0
    val view = new DynamicView(fields)

    println(this.fieldNames.mkString(", "))

    this.fieldNames.foreach{ name =>
      val o: AnyRef = record.get(this.fieldNames(i), this.outputSchema)

        val field = fields.find(f => f.n == name).get
        val value = ViewSerDe.deserializeField(field.t, o.asInstanceOf[String])
        println(value)
        view.put(field,value)
    }
//
//
//    while (i < this.fieldNames.length) {
//
//      val o: AnyRef = record.get(this.fieldNames(i), this.outputSchema)
//
//      // partition columns are returned as String
//      //   Check and convert to actual type.
//      this.outputSchema.get(i).getType match {
//        case HCatFieldSchema.Type.INT =>
//          if (o.isInstanceOf[String]) {
//            vals(i) = o.asInstanceOf[String].toInt
//          }
//          else {
//            vals(i) = o.asInstanceOf[Int]
//          }
//        case HCatFieldSchema.Type.TINYINT =>
//          if (o.isInstanceOf[String]) {
//            vals(i) = o.asInstanceOf[String].toInt.toByte
//          }
//          else {
//            vals(i) = o.asInstanceOf[Byte]
//          }
//        case HCatFieldSchema.Type.SMALLINT =>
//          if (o.isInstanceOf[String]) {
//            vals(i) = o.asInstanceOf[String].toInt.toShort
//          }
//          else {
//            vals(i) = o.asInstanceOf[Short]
//          }
//        case HCatFieldSchema.Type.BIGINT =>
//          if (o.isInstanceOf[String]) {
//            vals(i) = o.asInstanceOf[String].toLong
//          }
//          else {
//            vals(i) = o.asInstanceOf[Long]
//          }
//        case HCatFieldSchema.Type.BOOLEAN =>
//          if (o.isInstanceOf[String]) {
//            vals(i) = o.asInstanceOf[String].toBoolean
//          }
//          else {
//            vals(i) = o.asInstanceOf[Boolean]
//          }
//        case HCatFieldSchema.Type.FLOAT =>
//          if (o.isInstanceOf[String]) {
//            vals(i) = o.asInstanceOf[String].toFloat
//          }
//          else {
//            vals(i) = o.asInstanceOf[Float]
//          }
//        case HCatFieldSchema.Type.DOUBLE =>
//          if (o.isInstanceOf[String]) {
//            vals(i) = o.asInstanceOf[String].toDouble
//          }
//          else {
//            vals(i) = o.asInstanceOf[Double]
//          }
//        case HCatFieldSchema.Type.STRING =>
//          vals(i) = o
//        case HCatFieldSchema.Type.BINARY =>
//          if (o.isInstanceOf[String]) {
//            throw new RuntimeException("Cannot handle partition keys of type BINARY.")
//          }
//          else {
//            vals(i) = o.asInstanceOf[Array[Byte]]
//          }
//        case HCatFieldSchema.Type.ARRAY =>
//          if (o.isInstanceOf[String]) {
//            throw new RuntimeException("Cannot handle partition keys of type ARRAY.")
//          }
//          else {
//            vals(i) = o.asInstanceOf[List[Object]]
//          }
//        case HCatFieldSchema.Type.MAP =>
//          if (o.isInstanceOf[String]) {
//            throw new RuntimeException("Cannot handle partition keys of type MAP.")
//          }
//          else {
//            vals(i) = o.asInstanceOf[Map[Object, Object]]
//          }
//        case HCatFieldSchema.Type.STRUCT =>
//          if (o.isInstanceOf[String]) {
//            throw new RuntimeException("Cannot handle partition keys of type STRUCT.")
//          }
//          else {
//            vals(i) = o.asInstanceOf[List[Object]]
//          }
//        case _ =>
//          throw new RuntimeException("Invalid type " + this.outputSchema.get(i).getType +
//            " encountered.")
//      }
//
//      i += 1
//    }

//    view.set(fields(0),"test")

    view
  }

  override def getMaxFlinkTupleSize: Int = 100


}


object ViewInputFormat {

  def apply(view: View): ViewInputFormat = {
    val config = new Configuration()

//    val gen = TupleGeneric[T]

//    val product = gen.to(view.asInstanceOf[T])

    println(view.tableName)
    val dbName = view.tableName
    val tableName = dbName.split("\\.")(1)
    val databaseName = dbName.split("\\.")(0)

    new ViewInputFormat(
      view.fields.map{ f => SimpleField(f.n,f.t) },
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