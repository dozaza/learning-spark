
package shared_variable

import org.apache.spark.rdd.RDD

object WorkOnPartition {

  def readFromDb(rdd: RDD[String]): Unit = {
    // mapPartition is often used when you want to do some heavy operation such as db query,
    // mapPartition will give you data in one partition, so that you can save the cost on db connection.
    val converted = rdd.mapPartitions{ partition =>
      // Create db connection
      val keys = partition.toList
      // select some_thing from some_table where column in (keys)
      partition
    }

    rdd.mapPartitionsWithIndex { case(i, partitition) =>
      // i is the index of partition
      partitition
    }

    rdd.foreachPartition{ partition =>
      // Similar to mapPartition
    }
  }

  // This average implementation is better than the older one, cause this only create one tuple in each partition,
  // the older one will create a tuple for every element.
  def average(rdd: RDD[Int]): Double = {
    val pairRdd = rdd.mapPartitions{ partition =>
      val tuple = partition.foldLeft((0, 0)){ case((sum, count), d) =>
        (sum + d, count + 1)
      }
      List(tuple).toIterator
    }

    val (sum, count) = pairRdd.reduce{ case(t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)}
    if (0 == count) {
      0d
    } else {
      sum.toDouble / count
    }
  }

}
