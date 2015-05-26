name := "All Pairs Shortest Path"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

sparkVersion := "1.3.1"

spName := "arzavj/spark-all-pairs-shortest-path"

sparkComponents += "mllib"