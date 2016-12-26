name := "spark-test"

version := "0.0.x"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" % "spark-core_2.10" % "2.0.2",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.8.5",
    "au.com.bytecode" % "opencsv" % "2.4"
  )
}
