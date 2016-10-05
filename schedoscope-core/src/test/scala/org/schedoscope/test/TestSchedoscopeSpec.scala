package org.schedoscope.test

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Field._
import org.schedoscope.dsl.Parameter._
import test.views.{Click, ClickOfEC0101}


class TestSchedoscopeSpec extends SchedoscopeSpec {

  val ec0101Clicks = new Click(p("EC0101"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event01"),
      v(url, "http://ec0101.com/url1"))
    set(
      v(id, "event02"),
      v(url, "http://ec0101.com/url2"))
    set(
      v(id, "event03"),
      v(url, "http://ec0101.com/url3"))
  }

  val ec0106Clicks = new Click(p("EC0106"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event04"),
      v(url, "http://ec0106.com/url1"))
    set(
      v(id, "event05"),
      v(url, "http://ec0106.com/url2"))
    set(
      v(id, "event06"),
      v(url, "http://ec0106.com/url3"))
  }

  val click = putViewUnderTest {
    new ClickOfEC0101(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)
    }
  }

  //import the view under test to access it in the tests
  import click._

  "the spec" should "produce the right amount of rows" in {
    numRows() shouldBe 3
  }

  it should "contain some events" in {
    row(v(id) shouldBe "event01",
      v(url) shouldBe "http://ec0101.com/url1")
    row(v(id) shouldBe "event02",
      v(url) shouldBe "http://ec0101.com/url2")
  }

  it should "increment the row counter" in {
    startWithRow(2)
    row(v(id) shouldBe "event03",
      v(url) shouldBe "http://ec0101.com/url3")
  }


}

class TestReusableFixtures extends SchedoscopeSpec with ReusableFixtures {



  val ec0101Clicks = new Click(p("EC0101"), p("2014"), p("01"), p("01")) with rows

  val ec0106Clicks = new Click(p("EC0106"), p("2014"), p("01"), p("01")) with rows

  val click = new ClickOfEC0101(p("2014"), p("01"), p("01")) with LoadableView {
    basedOn(ec0101Clicks, ec0106Clicks)
    withResource()
  }

  import click._

  "the test" should "do" in {
    {
      import ec0101Clicks._
      set(
        v(id, "event01"),
        v(url, "http://ec0101.com/url1"))
      set(
        v(id, "event02"),
        v(url, "http://ec0101.com/url2"))
      set(
        v(id, "event03"),
        v(url, "http://ec0101.com/url3"))
    }
    {
      import ec0106Clicks._
      set(
        v(id, "event01"),
        v(url, "http://ec0101.com/url1"))
      set(
        v(id, "event02"),
        v(url, "http://ec0101.com/url2"))
      set(
        v(id, "event03"),
        v(url, "http://ec0101.com/url3"))
    }
    then(click)
    numRows() shouldBe 3
  }


  it should "do this" in {
    {
      import ec0101Clicks._
      set(
        v(id, "event01"),
        v(url, "url1"))
      set(
        v(id, "event02"),
        v(url, "url2"))
      set(
        v(id, "event03"),
        v(url, "url3"))
    }
    then(click)
    row(v(id) shouldBe "event01",
      v(url) shouldBe "url1")
    row(v(id) shouldBe "event02",
      v(url) shouldBe "url2")
  }




}