package pair_rdd

import org.apache.spark.rdd.RDD

object JoinPairRDD {

  def innerJoin(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {
    // only join keys both exists in 2 rdds, and return key -> (value_in_rdd1, value_in_rdd2)
    rdd1.join(rdd2)
  }

  def rightJoin(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Option[Int], Int))] = {
    rdd1.rightOuterJoin(rdd2)
  }

  def leftJoin(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Int, Option[Int]))] = {
    rdd1.leftOuterJoin(rdd2)
  }

}
