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

  val urlPattern = """(https?|file|ftp|gopher|telnet)://[\w\d:#@%/;$()~_?+-=.&]*""".r

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

      // TODO: remove Quads with same subject and object, but different predicate or context
      //      val dedupRDD = quadRDD.map(x => (x._1, x._3)).distinct()

      // extract all object literals
      val literalRDD = quadRDD.map(_._3).filter(isLiteral).map(toLiteral)

      // analyze use of language and data type in Literals
      val languages = literalRDD.map(l => (l.getLanguageTag, 1)).reduceByKey(_ + _).collect().toSeq
      val dataTypes = literalRDD.map(l => (l.getDatatype, 1)).reduceByKey(_ + _).collect().toSeq
      println(s"languages: $languages")
      println(s"dataTypes: $dataTypes")

      // extract only English text
      val englishTextRDD = literalRDD.filter(isLanguage("en")).map(_.getLabel)

      println("# all literals: " + literalRDD.count())
      println("# english text: " + englishTextRDD.count())

      // analyze text duplicates
      val duplicateTextRDD = englishTextRDD.map((_, 1)).reduceByKey(_ + _)
      duplicateTextRDD.takeOrdered(5)(Ordering.by(-_._2)).foreach(println)

      // tokenize text lines
      val tokenizedRDD = englishTextRDD.distinct().map(tokenize)
      tokenizedRDD.saveAsTextFile(s"$output/enLit")

      val allWordRDD = tokenizedRDD.flatMap(_.map((_, 1))).reduceByKey(_ + _)
      println("#words: " + allWordRDD.count())
      allWordRDD.sortBy(_._2, false).take(20).foreach(println)

      val htf = new HashingTF()
      val tf = htf.transform(tokenizedRDD)

      val idf = new IDF()
      val model = idf.fit(tf)
      val tfidf = model.transform(tf)

      println(s"#terms: ${tf.count()}")
      tf.take(5).foreach { vec => println(s"vector: $vec") }

      println(s"tfidf-size: ${tfidf.count()}")
      tfidf.take(5).foreach { v => println(s"tfidf: $v") }

      tf.saveAsTextFile(s"$output/tf")

    } finally {
      sc.stop()
    }
  }

  val stopwords = """
    a, able, about, across, after, all, almost, also, am, among, an, and, any, are, as, at, be, because, been,
    but, by, can, cannot, could, dear, did, do, does, either, else, ever, every, for, from, get, got, had, has, 
    have, he, her, hers, him, his, how, however, i, if, in, into, is, it, its, just, least, let, like, likely, 
    may, me, might, most, must, my, neither, no, nor, not, of, off, often, on, only, or, other, our, own, rather, 
    said, say, says, she, should, since, so, some, than, that, the, their, them, then, there, these, they, this, 
    tis, to, too, twas, us, wants, was, we, were, what, when, where, which, while, who, whom, why, will, with, 
    would, yet, you, your
    """.trim().split("""\W+""").toSet

  // remove punctuation and split string
  def tokenize(line: String): Seq[String] =
    removeUrl(line.toLowerCase()).split("""\W+""") // split on non-word characters (e.g. punctuation)
      .filter(_.size >= 2)
      .filterNot(stopwords.contains(_))
      .toSeq

  private def removeUrl = (x: String) => urlPattern.replaceAllIn(x, "")
}