package util

import scala.util.Failure
import scala.util.Try
import org.apache.spark.rdd.RDD
import org.semanticweb.yars.nx.Literal
import org.semanticweb.yars.nx.Node
import org.semanticweb.yars.nx.parser.NxParser
import org.semanticweb.yars.nx.namespace.RDF

object NQuadUtil {

  type QUAD = (Node, Node, Node, Node)

  def getLiterals(rdd: RDD[QUAD]) = rdd.map(_._3).flatMap { case l: Literal => Some(l); case _ => None }
  def getTypes(rdd: RDD[QUAD]) = rdd.flatMap { case (s, p, o, c) if RDF.TYPE.equals(p) => Some(o); case _ => None }

  /**
   * Parse NQuads from a text RDD.
   *
   * @param textRDD
   * @return a RDD of Node tuples.
   */
  def parse = (textRDD: RDD[String]) => textRDD.map(tryParse).filter(_.isSuccess).map(_.get)

  /**
   * Collect all errors when parsing NQuads from a text RDD.
   *
   * @param textRDD
   * @return a RDD of parse errors.
   */
  def parseErrors = (textRDD: RDD[String]) => textRDD.map(tryParse).flatMap({ case Failure(t) => Some(t); case _ => None })

  private def tryParse = (line: String) => Try(
    NxParser.parseNodes(line) match {
      case Array(s, p, o, c) => (s, p, o, c)
      case x: Array[Node]    => throw new IllegalArgumentException(s"not a valid NQuad: (${x.mkString(" ")})")
    })

}