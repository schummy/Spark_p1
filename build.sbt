version := "1.0"
 
scalaVersion := "2.10.6"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.2.0"


resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"