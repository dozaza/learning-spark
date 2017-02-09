package spark_sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext


object SparkSql {

  // "sql" is Hive sql, similar to native sql, ex: SELECT name, age FROM users
  def getFromHive(sql: String): Unit = {
    val builder = SparkSession.builder().enableHiveSupport()
    val hiveCtx = new HiveContext(builder)
    // rows is a RDD
    val rows = hiveCtx.sql(sql)
    val firstRow = rows.first
    println(firstRow.getString(0))
  }

  // When use hive to parse json, we don't need to prepare a hive config file "hive-site.xml"
  def getFromJson(path: String, sql: String): Unit = {
    val hiveCtx = new HiveContext()
    // Create a temp hive db
    val data = hiveCtx.jsonFile(path)
    data.registerTempTable("data")
    val rows = hiveCtx.sql(sql)
    val firstRow = rows.first
    println(firstRow.getString(0))
  }

}

