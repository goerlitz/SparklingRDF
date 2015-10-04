

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfterAll
import org.semanticweb.yars.nx.Literal
import org.semanticweb.yars.nx.Node
import org.semanticweb.yars.nx.Resource
import scala.util.Success

class NQuadUtilTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sc = new SparkContext("local", "CountTest")

  val testQuads = Array("<http://cars.com/myCar> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://cars.com/Car> <http://cars.com> .")

  "The NQuadUtil" should "parse a NQuad correctly" in {
    val testQuadRDD = sc.parallelize(testQuads.seq)
    val parsedRDD = NQuadUtil.parse(testQuadRDD)

    parsedRDD.count() shouldEqual (1)

    parsedRDD.collect()(0) match {
      case (s, p, o, c) =>
        s shouldBe new Resource("http://cars.com/myCar")
        p shouldBe new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        o shouldBe new Resource("http://cars.com/Car")
        c shouldBe new Resource("http://cars.com")
    }
  }

  it should "succeed another test" in {
    val res = getClass.getResource("btc-2010-10k.gz")
    val textRDD = sc.textFile(res.toString)
    val parsedRDD = NQuadUtil.parse(textRDD)

    //    val literals = parsedRDD.map { case (s, p, o, c) => o }.map { case lit: Literal => lit.getLabel }
    val literals = parsedRDD.map { case (s, p, o, c) => o }.filter { case lit: Literal => true; case _ => false } map { x => x.getLabel }
    println(literals.count())
  }
  
  // TODO: test wrong NQuad
  it should "fail for invalid quads" in {
    val testQuadRDD = sc.parallelize(Array("_:me _:loves http://some.thing").seq)
    val parsedRDD = NQuadUtil.parse(testQuadRDD)
    
    parsedRDD.count() shouldEqual(0)
  }

  override def afterAll() = {
    sc.stop()
  }
}
