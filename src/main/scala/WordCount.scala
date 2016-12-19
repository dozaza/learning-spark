import java.io.File

import Main.getClass
import org.apache.spark.rdd.RDD

/** Copyright © 2013-2016 DataYes, All Rights Reserved. */


object WordCount {

  def count(rdd: RDD[String]): Unit = {
    val wordCount1 = getWordCount(rdd)
    getWordCountInBook(rdd)

    println(s"My count $wordCount1")
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
