name := "All Pairs Shortest Path"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1"
libraryDependencies  ++= Seq(
  // other dependencies here
  "org.scalanlp" %% "breeze" % "0.11.2",
  // native libraries are not included by default. add this if you want them (as of 0.7)
  // native libraries greatly improve performance, but increase jar sizes.
  "org.scalanlp" %% "breeze-natives" % "0.11.2"
)

sparkVersion := "1.3.1"

spName := "arzavj/spark-all-pairs-shortest-path"

sparkComponents += "mllib"