
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

    val values = rdd.flatMap { line =>
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

    // Here will cause calculation
    println(blankLines.value)

    values
  }

  def getValidAndCount(sc: SparkContext, path: String, index: Int)(validate: String => Boolean): RDD[String] = {
    val rdd = sc.textFile(path)
    // Variable in each partition is standalone, so whatever changes done on a partition will never be applied on another partition.
    // So to do a count for example, we should use accumulator. See more for new api, AccumulatorV2
    val blankLines = sc.longAccumulator
    val invalidNumbers = sc.longAccumulator

    val values = rdd.flatMap { line =>
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

    // Better not to use accumulator in RDD transformation, cause this accumulator may be update several times
    // Accumulator in RDD transformation is used for debug, for example: error count.
    val validValues = values.filter{v =>
      val isValid = validate(v)
      if (!isValid) {
        invalidNumbers.add(1L)
      }
      isValid
    }

    println(blankLines.value)
    println(invalidNumbers.value)

    validValues
  }


}
