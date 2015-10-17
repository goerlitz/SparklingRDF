package model

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.rdd.RDD
import org.semanticweb.yars.nx.Literal
import org.semanticweb.yars.nx.Node

import util.FileUtil
import util.NQuadUtil

object TextAnalysis {

  val output = "output/"

  def onlyLiteral = (rdd: RDD[Node]) => rdd.flatMap(_ match { case l: Literal => Some(l); case _ => None })
  def isLiteral = (n: Node) => n match { case l: Literal => true; case _ => false }
  def toLiteral = (n: Node) => n match { case l: Literal => l }
  def isLanguage(lang: String) = (l: Literal) => l.getLanguageTag match { case `lang` => true; case _ => false }

  def main(args: Array[String]): Unit = {

    // delete old output data
    FileUtil.rmrf(new File(output))

    // TODO: should not set local
    val sc = new SparkContext("local", "Text Analytics")

    try {

      val dataFile = args(0)

      val textRDD = sc.textFile(dataFile)
      val quadRDD = NQuadUtil.parse(textRDD)

      // extract all object literals
      val literalRDD = quadRDD.map(_._3).filter(isLiteral).map(toLiteral)

      // analyze use of language and data type in Literals
      val languages = literalRDD.map(l => (l.getLanguageTag, 1)).reduceByKey(_ + _).collect().toSeq
      val dataTypes = literalRDD.map(l => (l.getDatatype, 1)).reduceByKey(_ + _).collect().toSeq
      println(s"languages: $languages")
      println(s"dataTypes: $dataTypes")

      // extract only English text
      val englishTextRDD = literalRDD.filter(isLanguage("en")).map(_.getLabel)

      englishTextRDD.saveAsTextFile(s"$output/enLit")

      println("# all literals: " + literalRDD.count())
      println("# english text: " + englishTextRDD.count())

      // analyze text duplicates
      val duplicateTextRDD = englishTextRDD.map((_, 1)).reduceByKey(_ + _)
      duplicateTextRDD.takeOrdered(5)(Ordering.by(-_._2)).foreach(println)

      // compute document vectors
      val documents = englishTextRDD.map(split)

      val htf = new HashingTF()
      val tf = htf.transform(documents)

      val idf = new IDF()
      val model = idf.fit(tf)
      val tfidf = model.transform(tf)

      println(s"size: ${tf.count()}")
      tf.take(5).foreach { vec => println(s"vector: $vec") }

      println(s"tfidf-size: ${tfidf.count()}")
      tfidf.take(5).foreach { v => println(s"tfidf: $v") }

      tf.saveAsTextFile(s"$output/tf")

    } finally {
      sc.stop()
    }
  }

  // remove punctuation and split string
  def split = (x: String) => x.toLowerCase().replaceAll("""[\p{Punct}]""", " ").split(" ").toSeq

  def removeUrl = (x: String) => ???
}