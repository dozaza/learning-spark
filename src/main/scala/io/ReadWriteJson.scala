package io


import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ReadWriteJson {

  def read[T](input: Seq[String], clazz: Class[T], sc: SparkContext): RDD[T] = {
    val mapper = new ObjectMapper()
    val seq = input.flatMap { record =>
      try {
        Some(mapper.readValue(record, clazz))
      } catch {
        case e: Exception => None
      }
    }
    sc.parallelize(seq)
  }

  def write[T](rdd: RDD[T], path: String): Unit = {
    val mapper = new ObjectMapper()
    rdd.map(mapper.writeValueAsString).saveAsTextFile(path)
  }

}
