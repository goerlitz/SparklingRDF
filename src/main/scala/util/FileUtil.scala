package util

import java.io.File
import org.apache.spark.rdd.RDD

object FileUtil {

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