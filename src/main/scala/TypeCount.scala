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

      val qp = QuadProcessor(lineRDD)

      val pCount = qp.predCount
      val tCount = qp.typeCount

      saveKV(pCount, "output/predCount")
      saveKV(tCount, "output/typeCount")

      qp.getLiterals.saveAsTextFile("output/literals")

      qp.typeJoin.saveAsTextFile("output/typeJoin")

    } finally {
      sc.stop()
    }
  }

  def rmrf(root: File): Unit = {
    if (root.isFile) root.delete()
    else if (root.exists) {
      root.listFiles foreach rmrf
      root.delete()
    }
  }

  def saveKV(countRDD: RDD[(String, Int)], file: String) = {
    countRDD.map { case (k, v) => s"$k $v" }.saveAsTextFile(file)
  }

}
