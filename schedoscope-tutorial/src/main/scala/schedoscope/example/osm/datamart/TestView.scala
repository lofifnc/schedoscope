package schedoscope.example.osm.datamart

import org.apache.flink.api.scala.ExecutionEnvironment
import org.schedoscope.dsl.View
import org.schedoscope.dsl.flink.{FlinkTransformation, ViewInputFormat, SimpleField, DynamicView}
import org.schedoscope.dsl.views.{Id, JobMetadata}
import org.schedoscope.test.test
import schedoscope.example.osm.datahub.Shops
import org.apache.flink.api.scala._

case class TestView() extends View
    with Id
    with JobMetadata {

    implicit def viewToDataSet(view: View) : DataSet[DynamicView] = {
      val flinkEnv = ExecutionEnvironment.getExecutionEnvironment
      val input = ViewInputFormat(view).asFlinkTuples()
      flinkEnv.createInput(input)
    }

    val shopName = fieldOf[String]("The name of the profiled shop")
    val shopType = fieldOf[String]("The type of shop, as given by OSM")
    val area = fieldOf[String]("A geoencoded area string")
    val cntCompetitors = fieldOf[Int]("The number of competitors in the area (shops of the same type)")
    val cntRestaurants = fieldOf[Int]("The number of restaurants in the area")
    val cntTrainstations = fieldOf[Int]("The number of trainstations in the area")

    val shops = dependsOn { () => Shops() }
//    val shop = DynamicView(s)
//    shop.set(s.map, Map("test"->"test"))
//    shop.set(s.area, "boston")
//    shop.set(s.id, "1234")
//    val shop1 = DynamicView(s)
//    shop1.set(s.area, "san franciso")
//    val shop2 = DynamicView(s)
//    shop2.set(s.area, "new york")

//    val stream = flinkEnv.fromColle

  transformVia(() =>
    FlinkTransformation(() => {
      val shopsSet = shops().map { view => {
        //      @transient val v = new Shops()
        //      view.set(v.shopName, "santa barbara")
        view.created_by = "me"
        view
      }
      }
    }))
}

object TestView {
  val string = "check"
}


