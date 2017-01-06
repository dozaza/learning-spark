
package shared_variable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BroadcastVariable {

  def broadcastVariable[T, V](rdd: RDD[T], toShare: V)(implicit sc: SparkContext): Unit = {
    // Usually to share a large data for every worker
    // broadcast is read-only, any modification on it will only occurred on one worker
    val broadcasted = sc.broadcast(toShare)
    // Do transformation or action on rdd by using broadcast variable.
    // rdd.filter(_ == broadcasted.value)
  }

}

