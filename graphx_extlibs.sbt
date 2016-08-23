name := "GraphX-Extlibs"

version := "1.0"
parallelExecution in Test := false
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

