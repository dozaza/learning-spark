package PairRDD

import org.apache.spark.rdd.RDD

object AggratePairRDD {

  def meanByKey(rdd: RDD[(Int, Int)]): RDD[(Int, Double)] = {
    rdd.mapValues(v => (v, 1)).reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)).mapValues{case(sum, count) => sum / count.toDouble}
  }

  def combineByKey(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {
    // Calculate every key's sum & count of value
    rdd1.combineByKey(
      i => (i, 1),    // initial value
      (t: (Int, Int), i) => (t._1 + i, t._2 + 1),   // apply on every element in partition
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._2, t1._2 + t2._2)    // apply between partitions
    )
  }

}
