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
  val rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  //  rdfParser.setRDFHandler(collector)

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

      //      println(s"quad count: ${quadRDD.count()}")
      //      quadRDD.take(10).foreach(println)

      //      // count types
      //      val typeRDD = quadRDD
      //        .filter { case (s, p, o, c) => rdfType.equalsIgnoreCase(p.toString()) }
      //        .map { case (s, p, o, c) => (o, 1) }
      //        .reduceByKey(_ + _)
      //        .sortBy(_._2, false)
      //      println(s"type count: ${typeRDD.count()}")
      //      typeRDD.take(10).foreach(println)

      //      val terms = lineRDD.flatMap { line => getType(line) }
      //      val res = terms.map((_, 1)).reduceByKey(_ + _).sortByKey()
      //      //      val res = sc.makeRDD(terms.countByValue().map(kv => s"${kv._1},${kv._2}").toSeq)
      //      val res2 = res.map { case (key, count) => (count, key) }.sortByKey(false).map { case (key, value) => s"$key $value" }
      //      val res3 = res.keyBy(_._2).sortByKey(false).map(_._2)
      //
      //      res3.saveAsTextFile(output)
    } finally {
      sc.stop()
    }
  }

  // TODO: use Try as results
  def parseQuad(line: String): (Node, Node, Node, Node) = {
    new NxParser().parse(Iterator(line)).next() match {
      case Array(s, p, o, c) => (s, p, o, c)
      case _                 => throw new UnsupportedOperationException("not a quad")
    }
  }

  def getProperty(line: String): Array[String] = {
    // nxparser is faster but does not check correctness of statements
    Array(new NxParser().parse(Iterator(line)).next()(1).toString())
  }

  def getType(line: String): Array[String] = {
    val node = new NxParser().parse(Iterator(line)).next()
    if (rdfType.equals(node(1).toString()))
      Array(node(2).toString())
    else
      Array()
  }

  //    try {
  //      // use Sesame's NQAD parser (slower due to statement validation)
  //    	collector.clear()
  //      rdfParser.parse(new StringReader(line), "")
  //      Array(collector.getStatements.iterator().next().getPredicate().stringValue())
  //    } catch {
  //      case e: RDFParseException => println(s"Parse error: $e - $line"); return Array()
  //    }

  //  def rmrf(file: File): Unit = {
  //    if (file.isDirectory()) file.listFiles foreach rmrf
  //    if (file.exists && file.delete == false) throw new RuntimeException(s"Deleting $file failed!")
  //  }

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
    
    predCountRDD.saveAsTextFile("output/predCount")
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