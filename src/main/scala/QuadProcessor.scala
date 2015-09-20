import org.apache.spark.rdd.RDD
import org.semanticweb.yars.nx.Literal
import org.semanticweb.yars.nx.Node
import org.semanticweb.yars.nx.parser.NxParser

object QuadProcessor {

  type QuadRDD = RDD[(Node, Node, Node, Node)]

  def apply(textRDD: RDD[String]) = {
    val quadRDD = textRDD.map { line =>
      // TODO: use Try as results
      NxParser.parseNodes(line) match {
        case Array(s, p, o, c) => (s, p, o, c)
        case _                 => throw new UnsupportedOperationException("not a quad")
      }
    }
    new QuadProcessor(quadRDD)
  }
  val rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
}

class QuadProcessor(quadRDD: QuadProcessor.QuadRDD) {

  import QuadProcessor.rdfType

  /**
   * Count all predicates and order by descending value.
   */
  def predCount = quadRDD
    .map { case (s, p, o, c) => (p.getLabel(), 1) }
    .reduceByKey(_ + _)
    .sortBy(_._2, false)

  def typeCount = quadRDD
    .filter { case (s, p, o, c) => rdfType.equalsIgnoreCase(p.getLabel()) }
    .map { case (s, p, o, c) => (o.getLabel(), 1) }
    .reduceByKey(_ + _)
    .sortBy(_._2, false)

  def getLiterals = quadRDD
    .map { case (s, p, o, c) => o }
    .filter {
      case o: Literal => true
      case _          => false
    }

}