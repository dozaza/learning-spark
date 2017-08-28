package spark_sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object SparkSql {

  def getFromHive(sql: String)(implicit sc: SparkContext): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("SparkSql hive test")
      .getOrCreate()

    import spark.sql

    val df = sql(sql)
    val firstRow = df.first
    println(firstRow.getString(0))
  }

  def getFromJson(path: String, sql: String): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSql json test")
      .getOrCreate()

    val df = spark.read.json(path)
    df.createTempView("data")

    val sqlDf = spark.sql(sql)
    val firstRow = sqlDf.first
    println(firstRow.getString(0))
  }

}

