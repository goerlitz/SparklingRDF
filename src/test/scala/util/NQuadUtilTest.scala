package util

import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class NQuadUtilTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sc = new SparkContext("local", "CountTest")

  "The NQuadUtil" should "parse valid NQuads" in {
    val validNQuads = """
      _:Bob _:knows _:Tom _:MyFriends .
      <http://ex.com/Bob> <http://ex.com/knows> "Tom" <http://ex.com/Friends> .
      """.trim.split('\n').toSeq

    val textRDD = sc.parallelize(validNQuads)

    val quadRDD = NQuadUtil.parse(textRDD)
    quadRDD.count() shouldEqual (validNQuads.length)

    val errors = NQuadUtil.parseErrors(textRDD)
    errors.count() shouldEqual (0)
  }

  it should "fail for invalid NQuads" in {

    val invalidNQuads = """
      _:Bob .
      _:Bob _:knows .
      _:Bob _:knows _:Tom .
      _:Bob _:knows _:Tom _:MyFriends _:Other .
      """.trim.split('\n').toSeq

    val textRDD = sc.parallelize(invalidNQuads)

    val quadRDD = NQuadUtil.parse(textRDD)
    quadRDD.count() shouldEqual (0)

    val errors = NQuadUtil.parseErrors(textRDD)
    errors.count() shouldEqual (invalidNQuads.length)
  }

  override def afterAll() = {
    sc.stop()
  }
}
