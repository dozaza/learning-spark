
package pair_rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object PartitionPairRDD {

  // ex: partition = new HashPartitioner(100)
  def partition[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], partitioner: Partitioner): RDD[(K, V)] = {
    // The reason to do partition on pair rdd is to avoid data shuffle
    // Pair rdd is partition by its' keys
    // Once partition finished and cached, we can do some job on keys without shuffling data, such as join.
    rdd.partitionBy(partitioner).persist()
  }

  def join[K: ClassTag, V: ClassTag](partitioned: RDD[(K, V)], toJoin: RDD[(K, V)]): RDD[(K, (V, V))] = {
    // Only the pair rdd "toJoin" will be shuffled cause the pair "partitioned" is partitioned
    // Attention: if "partitioned" hasn't been persisted, each time using this pair will cause shuffle4
    partitioned.join(toJoin)
  }

  def mapValue[K: ClassTag, V: ClassTag](partitioned: RDD[(K, V)]): RDD[(K, String)] = {
    // mapValue will not destroy the partition info on a partitioned pair rdd, but map will
    // same thing on flatMapValue & flatMap
    partitioned.mapValues(v => v.toString)
  }

  def pageRank(partitioned: RDD[(String, Seq[String])]): RDD[(String, Double)] = {
    // We use mapValues so initRanks restore its parent's("partitioned") partition info.
    val initRanks = partitioned.mapValues(v => 1.0)
    (0 until 10).foldLeft(initRanks){ case(ranks, _) =>
      // Action join won't consume too much cause pair rdd is partitioned
      val contributions = partitioned.join(ranks).flatMap { case(pageId, (links, rank)) =>
        links.map(dest => (dest, rank / links.size))
      }

      // reduceByKey will create partition info and mapValues won't destroy it,
      // so when the result of mapValues to join "partitioned" won't cause shuffle
      contributions.reduceByKey((c1, c2) => c1 + c2).mapValues(v => 0.15 + 0.85 * v)
    }
  }

}
