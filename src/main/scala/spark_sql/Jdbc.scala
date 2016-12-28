
package spark_sql

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

object Jdbc {

  // sql should have ranger comparison, ex: SELECT * FROM user WHERE ? <= age and age <=?;
  def getFromDb(sc: SparkContext, host :String, sql: String): Unit = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val connection = DriverManager.getConnection(host)

    // When use JdbcRDD, it's better to set "lowerBound" & "upperBound" for a data range, or spark will get all data from different cluster, may cause performance problems
    val data = new JdbcRDD(sc, {() => connection}, sql, lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)
  }

  private def extractValues(r: ResultSet): (String, String) = {
    (r.getString(1), r.getString(2))
  }

}
