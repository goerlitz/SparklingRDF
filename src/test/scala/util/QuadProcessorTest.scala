package util

import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import util.QuadProcessor

class CountTest extends FlatSpec with Matchers {

  val sc = new SparkContext("local", "CountTest")

  val res = getClass.getResource("btc-2010-10k.gz")
  val textRDD = sc.textFile(res.toString)

  val qp = QuadProcessor(textRDD)

  "The quad processor" should "count 10.000 entries" in {
    textRDD.count() should equal(10000)
  }

  it should "count 135 types" in {
    qp.predCount.count() should equal(135)
  }

  it should "count 42 pedicates" in {
    qp.typeCount.count() should equal(42)
  }

}
