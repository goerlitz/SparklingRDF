package util

import org.apache.spark.rdd.RDD
import org.semanticweb.yars.nx.parser.NxParser
import scala.util.Try
import org.semanticweb.yars.nx.Node
import scala.util.Failure

object NQuadUtil {

  /**
   * Parse NQuads from a text RDD.
   *
   * @param textRDD
   * @return a RDD of Node tuples.
   */
  def parse(textRDD: RDD[String]): RDD[(Node, Node, Node, Node)] = textRDD.map(tryParse).filter(_.isSuccess).map(_.get)

  def getParseFailures(textRDD: RDD[String]): RDD[Throwable] = textRDD.map(tryParse).filter(_.isFailure).map {
    case Failure(t) => t
  }

  def tryParse = (line: String) =>
    Try(
      NxParser.parseNodes(line) match {
        case Array(s, p, o, c) => (s, p, o, c)
        case x: Array[Node]    => throw new IllegalArgumentException(s"not a valid NQuad: (${x.mkString(" ")})")
      })

}