import java.io.File
import java.io.StringReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.openrdf.rio.RDFFormat
import org.openrdf.rio.RDFParseException
import org.openrdf.rio.Rio
import org.openrdf.rio.helpers.StatementCollector
import org.semanticweb.yars.nx.parser.NxParser
import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.Success
import org.semanticweb.yars.nx.Node
import scala.util.Failure
import org.apache.spark.rdd.RDD

object TypeCount {

  //  val rdfParser = Rio.createParser(RDFFormat.NQUADS);
  //  val collector = new StatementCollector()
  //  rdfParser.setRDFHandler(collector)

  val rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

  type QuadRDD = RDD[(Node, Node, Node, Node)]

  def main(args: Array[String]): Unit = {

    // delete old output data
    val output = "output/"
    rmrf(new File(output))
    println(s"deleted $output")

    // TODO: should not set local
    val sc = new SparkContext("local", "Type Count")

    try {

      val dataFile = args(0)

      println(s"loading file $dataFile")

      // load quad file and split in <S, P, O, C>
      val lineRDD = sc.textFile(dataFile)
      val quadRDD = lineRDD.map(line => parseQuad(line))

      quadRDD.cache()

      countPredicates(quadRDD)
      countTypes(quadRDD)

    } finally {
      sc.stop()
    }
  }

  // TODO: use Try as results
  def parseQuad(line: String): (Node, Node, Node, Node) = {
    NxParser.parseNodes(line) match {
      case Array(s, p, o, c) => (s, p, o, c)
      case _                 => throw new UnsupportedOperationException("not a quad")
    }
  }

  def rmrf(root: File): Unit = {
    if (root.isFile) root.delete()
    else if (root.exists) {
      root.listFiles foreach rmrf
      root.delete()
    }
  }

  def countPredicates(quadRDD: QuadRDD) = {
    val predRDD = quadRDD.map { case (s, p, o, c) => (p.getLabel(), 1) }
    val predCountRDD = predRDD.reduceByKey(_ + _).sortBy(_._2, false)

    predCountRDD.map { case (k, v) => s"$k $v" }.saveAsTextFile("output/predCount")
  }

  def countTypes(quadRDD: QuadRDD) = {
    val typeCountRDD = quadRDD
      .filter { case (s, p, o, c) => rdfType.equalsIgnoreCase(p.getLabel()) }
      .map { case (s, p, o, c) => (o.getLabel(), 1) }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    typeCountRDD.saveAsTextFile("output/typeCount")
  }

}
