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
package schedoscope.example.osm.datamart

import java.sql.DriverManager

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Field._
import org.schedoscope.dsl.flink.DynamicView
import org.schedoscope.test.{rows, test}
import schedoscope.example.osm.datahub.{Restaurants, Shops, Trainstations}


case class ShopProfilesTest() extends FlatSpec
  with Matchers {

  Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
  val dbConnection = DriverManager.getConnection("jdbc:derby:memory:TestingDB;create=true")

  val shops = new Shops() with rows {
    set(v(id, "122546"),
      v(shopName, "Netto"),
      v(shopType, "supermarket"),
      v(area, "t1y87ki"),
      v(size, 10),
      v(mapF, Map("test" -> "hey")))
    set(v(id, "274850441"),
      v(shopName, "Schanzenbaeckerei"),
      v(shopType, "bakery"),
      v(area, "t1y87ki"),
      v(size, 12))
    set(v(id, "279023080"),
      v(shopName, "Edeka Linow"),
      v(shopType, "supermarket"),
      v(area, "t1y77d8"),
      v(size,13))
  }

  val restaurants = new Restaurants() with rows {
    set(v(id, "267622930"),
      v(restaurantName, "Cuore Mio"),
      v(restaurantType, "italian"),
      v(area, "t1y06x1"))
    set(v(id, "288858596"),
      v(restaurantName, "Jam Jam"),
      v(restaurantType, "japanese"),
      v(area, "t1y87ki"))
    set(v(id, "302281521"),
      v(restaurantName, "Walddoerfer Croque Cafe"),
      v(restaurantType, "burger"),
      v(area, "t1y17m9"))
  }

  val trainstations = new Trainstations() with rows {
    set(v(id, "122317"),
      v(stationName, "Hagenbecks Tierpark"),
      v(area, "t1y140d"))
    set(v(id, "122317"),
      v(stationName, "Boenningstedt"),
      v(area, "t1y87ki"))
  }

  "datamart.ShopProfiles" should "load correctly from datahub.shops, datahub.restaurants, datahub.trainstations" in {
    new ShopProfiles() with test {
      configureExport("schedoscope.export.jdbcConnection", "jdbc:derby:memory:NullDB;create=true")
      configureExport("schedoscope.export.dbUser", null)
      configureExport("schedoscope.export.dbPass", null)

      basedOn(shops, restaurants, trainstations)
      then()
      numRows shouldBe 3
      row(v(id) shouldBe "122546",
        v(shopName) shouldBe "Netto",
        v(shopType) shouldBe "supermarket",
        v(area) shouldBe "t1y87ki",
        v(cntCompetitors) shouldBe 1,
        v(cntRestaurants) shouldBe 1,
        v(cntTrainstations) shouldBe 1)
    }
  }

  it should "export data to JDBC as well" in {
    new ShopProfiles() with test {
      configureExport("schedoscope.export.jdbcConnection", "jdbc:derby:memory:TestingDB")
      configureExport("schedoscope.export.dbUser", null)
      configureExport("schedoscope.export.dbPass", null)

      basedOn(shops, restaurants, trainstations)

      then()

      numRows shouldBe 3
    }

    val statement = dbConnection.createStatement()
    val resultSet = statement.executeQuery("SELECT COUNT(*) FROM TEST_SCHEDOSCOPE_EXAMPLE_OSM_DATAMART_SHOP_PROFILES")
    resultSet.next()

    resultSet.getInt(1) shouldBe 3

    resultSet.close()
    statement.close()
  }

  it should "flinky flink" in {
    new TestView() with test {
      basedOn(shops)
      then()
    }

  }

  "flink test" should "show something with" in {
    new ShopProfiles() {

      //
      val flinkEnv = ExecutionEnvironment.getExecutionEnvironment

      val string = "hallo"

      //            val input = ViewInputFormat(ShopProfiles())


      val shop = DynamicView(test())
      shop.set(area, "boston")
      shop.set(id, "test")
      val shop1 = DynamicView(test())
      shop1.set(area, "san franciso")
      val shop2 = DynamicView(test())
      shop2.set(area, "new york")

      val stream = flinkEnv.fromCollection(List(shop, shop1, shop2))

      val shops = stream.map {
        view => {
          @transient val v = new Shops()
          view.set(v.shopName, "test")
          //          println(string)
          //          view.set(area,"test")
          view
        }
      }

      shops.print()
    }

  }

  "flink test" should "closure" in {

    val flinkEnv = ExecutionEnvironment.getExecutionEnvironment

    val string = "hallo"

    //      val input = ViewInputFormat(ShopProfiles())

    val stream = flinkEnv.fromCollection(List(1, 2, 3))

    val shops = stream.map {
      view => {
        println(string)
        //          view.set(area,"test")
        view
      }
    }

    shops.print()


  }

}

