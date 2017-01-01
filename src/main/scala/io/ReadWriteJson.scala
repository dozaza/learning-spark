package io


import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object ReadWriteJson {

  //TODO compile error, maybe should use ClassTag
  def read[T: ClassTag](input: Seq[String], clazz: Class[T], sc: SparkContext): RDD[T] = {
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

  def write[T: ClassTag](rdd: RDD[T], path: String): Unit = {
    val mapper = new ObjectMapper()
    rdd.map(mapper.writeValueAsString).saveAsTextFile(path)
  }

}
