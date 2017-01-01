
package shared_variable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Accumulator {

  // Get value of a column and count blank lines
  def getAndCount(sc: SparkContext, path: String, index: Int): RDD[String] = {
    val rdd = sc.textFile(path)
    // Variable in each partition is standalone, so whatever changes done on a partition will never be applied on another partition.
    // So to do a count for example, we should use accumulator. See more for new api, AccumulatorV2
    val blankLines = sc.longAccumulator

    rdd.flatMap { line =>
      if ("" == line) {
        blankLines.add(1L)
        None
      } else {
        val values = line.split(", ")
        if (values.size >= index) {
          None
        } else {
          Some(values(index))
        }
      }
    }

  }

}
