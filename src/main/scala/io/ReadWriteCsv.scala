package io

import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

object ReadWriteCsv {

  def read(path: String, sc: SparkContext) = {
    val input = sc.textFile(path)
    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }

    val input2 = sc.wholeTextFiles(path)
    val result2 = input2.flatMap { case(_, txt) =>
      val reader = new CSVReader(new StringReader(txt))
      reader.readAll().map{ array =>
        // Convert to object
        array
      }
    }
  }

  def write[T](rdd: RDD[T], path: String): Unit = {
    rdd.map(o => List(o.toString).toArray).mapPartitions{ array =>
      val stringWriter = new StringWriter()
      val csvWriter = new CSVWriter(stringWriter)
      csvWriter.writeAll(array.toList)
      Iterator(stringWriter)
    }.saveAsTextFile(path)
  }
}
