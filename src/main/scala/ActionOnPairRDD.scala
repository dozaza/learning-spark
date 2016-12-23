import org.apache.spark.rdd.RDD

import scala.collection.Map

object ActionOnPairRDD {

  def countByKey(rdd: RDD[(Int, Int)]): Map[Int, Long] = {
    rdd.countByKey()
  }

  def collectAsMap(rdd: RDD[(Int, Int)]): Map[Int, Int] = {
    rdd.collectAsMap()
  }

  def lookup(rdd: RDD[(Int, Int)], key: Int): List[Int] = {
    rdd.lookup(key).toList
  }

}
