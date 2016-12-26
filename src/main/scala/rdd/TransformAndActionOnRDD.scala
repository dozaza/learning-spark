package rdd

import org.apache.spark.rdd.RDD

object TransformAndActionOnRDD {

  // All transformation on rdd will return a new rdd

  def filter[T](rdd: RDD[T], f: T => Boolean): RDD[T] = {
    rdd.filter(f)
  }

  def union[T](rdd1: RDD[T], rdd2: RDD[T]): RDD[T] = {
    rdd1.union(rdd2)
  }

  def pow(rdd: RDD[Double], n: Int): RDD[Double] = {
    rdd.map(i => Math.pow(i, n))
  }

  def splitWord(rdd: RDD[String]): RDD[String] = {
    rdd.flatMap(_.split(" "))
  }

  // All action on rdd will return a result value

  def printFirst10[T](rdd: RDD[T]): Unit = {
    rdd.take(10).foreach(println)
  }

  def printAll[T](rdd: RDD[T]): Unit = {
    // Important: method collect() will get all the data in rdd and save it into your local memory
    // so never use collect() on large size results.
    rdd.collect().foreach(println)
  }

  def reduce(rdd: RDD[Int]): Int = {
    rdd.reduce{ case(i1, i2) => i1 + i2 }
  }

  def fold(rdd: RDD[Int], init: Int): Int = {
    rdd.fold(init){ case(r, i) => r + i }
  }

  type IntContainer = { def i: Int }
  def aggregate[T <: IntContainer](rdd: RDD[T], init: Int): Int = {
    rdd.aggregate(init)(
      // action on a partition
      (r, e) => r + e.i,
      // action between 2 results of partition
      (r1, r2) => r1 + r2
    )
  }
}
