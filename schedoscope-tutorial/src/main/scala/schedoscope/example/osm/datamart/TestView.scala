package schedoscope.example.osm.datamart

import org.apache.flink.api.scala._
import org.schedoscope.dsl.View
import org.schedoscope.dsl.flink.FlinkTransformation
import org.schedoscope.dsl.flink.FlinkTransformation._
import org.schedoscope.dsl.views.{Id, JobMetadata}
import schedoscope.example.osm.datahub.Shops

case class TestView() extends View
  with Id
  with JobMetadata {


  val shopName = fieldOf[String]("The name of the profiled shop")
  val shopType = fieldOf[String]("The type of shop, as given by OSM")
  val area = fieldOf[String]("A geoencoded area string")
  val cntCompetitors = fieldOf[Int]("The number of competitors in the area (shops of the same type)")
  val cntRestaurants = fieldOf[Int]("The number of restaurants in the area")
  val cntTrainstations = fieldOf[Int]("The number of trainstations in the area")

  val shop = dependsOn { () => Shops() }
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
      val shopsSet = shop().map {
        v => {
          @transient val f = new Shops()
          //          view.set(v.shopName, "santa barbara")
          val size = v.get(f.size).getOrElse(0)
          v.set(f.size, size + 1)

          v
        }
      }

      shopsSet
    }))
}

object TestView {
  val string = "check"
}


