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
    filteredRdd.cache()

    val wordCount1 = getWordCount(filteredRdd)
    getWordCountInBook(filteredRdd)

    println(s"My count $wordCount1")

    sc.stop()

    println(sc.isStopped)
  }


  /**
    * Private methods
    */

  private def getFilePath(): String = {
    getClass.getResource("text.csv").getPath
  }

  private def getWordCount(rdd: RDD[String]): Int = {
    val count = rdd.map(_.split(",").length).sum()
    count.toInt
  }

  private def getWordCountInBook(rdd: RDD[String]): Unit = {
    val words = rdd.flatMap(l => l.split(","))
    val counts = words.map(w => (w, 1)).reduceByKey{ case(x, y) => x + y }
    val output = new File("./dev/count_result.csv")
    counts.saveAsTextFile(output.getAbsolutePath)
  }
}
