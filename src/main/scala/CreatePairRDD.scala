import org.apache.spark.rdd.RDD

object CreatePairRDD {

  def create(rdd: RDD[String]): RDD[(String, String)] = {
    rdd.map(s => (s.split(" ")(0), s))
  }

}
