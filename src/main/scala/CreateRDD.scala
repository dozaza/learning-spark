import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** Copyright © 2013-2016 DataYes, All Rights Reserved. */


object CreateRDD {

  def createByFile(context: SparkContext, path: String): RDD[String] = {
    context.textFile(path)
  }

  def createByList(context: SparkContext, list: List[String]): RDD[String] = {
    context.parallelize(list)
  }

}
