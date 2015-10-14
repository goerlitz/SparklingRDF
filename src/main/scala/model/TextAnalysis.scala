package model

import java.io.File
import org.apache.spark.SparkContext
import util.FileUtil
import util.QuadProcessor
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF

object TextAnalysis {

  val output = "output/"

  def main(args: Array[String]): Unit = {

    // delete old output data
    FileUtil.rmrf(new File(output))

    // TODO: should not set local
    val sc = new SparkContext("local", "Text Analytics")

    try {

      val dataFile = args(0)

      val lineRDD = sc.textFile(dataFile)
      val qp = QuadProcessor(lineRDD)

      val documents = qp.getLiterals.map(_.getLabel).map(_.split(" ").toSeq)

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
}