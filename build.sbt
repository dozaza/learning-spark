name := "spark-test"

version := "0.0.x"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" % "spark-core_2.10" % "2.1.0",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.8.5",
    "au.com.bytecode" % "opencsv" % "2.4",
    "org.apache.hadoop" % "hadoop-common" % "2.7.3",
    "org.apache.spark" % "spark-sql_2.10" % "2.1.0",
    "org.apache.spark" % "spark-hive_2.10" % "2.1.0"
  )
}
