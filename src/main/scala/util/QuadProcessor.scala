package util

import org.apache.spark.rdd.RDD
import org.semanticweb.yars.nx.Literal
import org.semanticweb.yars.nx.Node
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object QuadProcessor {

  val rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

  def apply(textRDD: RDD[String]) = new QuadProcessor(NQuadUtil.parse(textRDD))

}

class QuadProcessor(quadRDD: RDD[(Node, Node, Node, Node)]) {

  import QuadProcessor.rdfType

  def predCount = quadRDD.map(_._2).distinct().count()
  def typeCount = NQuadUtil.getTypes(quadRDD).distinct().count()

  def typeJoin = {
    val typePair = quadRDD
      .filter { case (s, p, o, c) => rdfType.equalsIgnoreCase(p.getLabel()) }
      .map { case (s, p, o, c) => (s, o) }
    val predPair = quadRDD
      .filter { case (s, p, o, c) => !rdfType.equalsIgnoreCase(p.getLabel()) }
      .map { case (s, p, o, c) => (s, p) }
    typePair
      .join(predPair)
      .map { case (k, v) => v }
      .groupByKey()
  }

}