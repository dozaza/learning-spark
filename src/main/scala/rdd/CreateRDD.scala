package rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD



object CreateRDD {

  def createByFile(context: SparkContext, path: String): RDD[String] = {
    context.textFile(path)
  }

  def createByList(context: SparkContext, list: List[String]): RDD[String] = {
    context.parallelize(list)
  }

}
