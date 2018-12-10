name := "RecursiveCTE"

version := "0.1"

scalaVersion := "2.11.12"


// JSQLParser 0.9
libraryDependencies += "com.github.jsqlparser" % "jsqlparser" % "0.9"

// Spark 2.1
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"