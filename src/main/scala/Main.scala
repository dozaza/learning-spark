import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object Main {

  def main(args: Array[String]) = {
    // setMaster(cluster's url)
    val conf = new SparkConf().setMaster("local").setAppName("Test Spark")
    val sc = new SparkContext(conf)

    val csvRdd = sc.textFile(getFilePath())
    val filteredRdd = csvRdd.filter(_.contains("XSHG"))
    // equals to persist()
    filteredRdd.cache()

    WordCount.count(filteredRdd)

    sc.stop()

    println(sc.isStopped)
  }


  /**
    * Private methods
    */

  private def getFilePath(): String = {
    getClass.getResource("text.csv").getPath
  }


}
