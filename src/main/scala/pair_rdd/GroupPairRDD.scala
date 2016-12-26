package pair_rdd

import org.apache.spark.rdd.RDD

object GroupPairRDD {

  def groupByKey(rdd: RDD[(Int, Int)]): RDD[(Int, List[(Int, Int)])] = {
    rdd.groupBy(_._1).mapValues(_.toList)
  }

  def cogroup(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Iterable[Int], Iterable[Int]))] = {
    // like first rdd1.groupBy(_._1).join(rdd2.groupBy(_._1))
    rdd1.cogroup(rdd2)
  }

}
