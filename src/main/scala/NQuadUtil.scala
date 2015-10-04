import org.apache.spark.rdd.RDD
import org.semanticweb.yars.nx.parser.NxParser
import scala.util.Try
import org.semanticweb.yars.nx.Node
import scala.util.Success

object NQuadUtil {

  /**
   * Parse NQuads from a text RDD.
   * 
   * @param textRDD
   * @return an RDD of Node tuples.
   */
  def parse(textRDD: RDD[String]): RDD[(Node, Node, Node, Node)] = textRDD.map(tryParse).filter(_.isSuccess).map(asTuple)

  def tryParse = (line: String) => Try(NxParser.parseNodes(line))
  def asTuple: Try[Array[Node]] => (Node, Node, Node, Node) = { case Success(Array(s, p, o, c)) => (s, p, o, c) }

}